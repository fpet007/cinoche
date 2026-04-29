const fs = require('fs');
const path = require('path');
const pLimit = require('p-limit');

const TMDB_API_KEY = process.env.TMDB_API_KEY || 'TA_CLE_ICI';

const DATA_PATH = path.join(__dirname, '../data/plex-upcoming.json');
const CACHE_PATH = path.join(__dirname, '../cache/tmdb-cache.json');

const limit = pLimit(5); // max 5 requêtes simultanées

const STUDIO_DELAYS = {
    DEFAULT: 45,
    FR: 121,
    UNIVERSAL: 25,
    WARNER: 35,
    DISNEY: 55
};

const STUDIOS = {
    UNIVERSAL: [33, 12248],
    WARNER: [174, 2734],
    DISNEY: [2, 420, 3, 1632],
    SONY: [5, 34],
    PARAMOUNT: [4, 60]
};

// ================= CACHE =================
function loadCache() {
    if (!fs.existsSync(CACHE_PATH)) return {};
    return JSON.parse(fs.readFileSync(CACHE_PATH, 'utf8'));
}

function saveCache(cache) {
    fs.writeFileSync(CACHE_PATH, JSON.stringify(cache, null, 2));
}

function isCacheValid(entry) {
    const ONE_DAY = 1000 * 60 * 60 * 24;
    return entry && (Date.now() - entry.timestamp < ONE_DAY);
}

// ================= HELPERS =================
function formatDate(date) {
    return date.toLocaleDateString('fr-FR', {
        day: 'numeric',
        month: 'long',
        year: 'numeric'
    });
}

function getDelay(details, isFrench) {
    const companyIds = details.production_companies?.map(c => c.id) || [];

    if (isFrench) return STUDIO_DELAYS.FR;
    if (companyIds.some(id => STUDIOS.UNIVERSAL.includes(id))) return STUDIO_DELAYS.UNIVERSAL;
    if (companyIds.some(id => STUDIOS.WARNER.includes(id))) return STUDIO_DELAYS.WARNER;
    if (companyIds.some(id => STUDIOS.DISNEY.includes(id))) return STUDIO_DELAYS.DISNEY;

    return STUDIO_DELAYS.DEFAULT;
}

// ================= MAIN =================
async function updateVOD() {
    try {
        const today = new Date();
        const currentMonth = today.getMonth();
        const currentYear = today.getFullYear();

        const startMonth = new Date(currentYear, currentMonth, 1);
        const endMonth = new Date(currentYear, currentMonth + 1, 0);

        const dateDebutFR = new Date(); dateDebutFR.setMonth(today.getMonth() - 5);
        const dateFinFR = new Date(); dateFinFR.setMonth(today.getMonth() - 3);

        const endpoints = [
            // FR
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&with_origin_country=FR&primary_release_date.gte=${dateDebutFR.toISOString().split('T')[0]}&primary_release_date.lte=${dateFinFR.toISOString().split('T')[0]}&sort_by=popularity.desc&page=1`,
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&with_origin_country=FR&primary_release_date.gte=${dateDebutFR.toISOString().split('T')[0]}&primary_release_date.lte=${dateFinFR.toISOString().split('T')[0]}&sort_by=popularity.desc&page=2`,

            // BLOCKBUSTERS
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&primary_release_date.gte=${dateDebutFR.toISOString().split('T')[0]}&sort_by=popularity.desc&page=1`,

            // EXTRA COVERAGE
            `https://api.themoviedb.org/3/movie/now_playing?api_key=${TMDB_API_KEY}&language=fr-FR&page=1`,
            `https://api.themoviedb.org/3/movie/upcoming?api_key=${TMDB_API_KEY}&language=fr-FR&page=1`
        ];

        // FETCH LISTES
        let allMovies = [];
        for (const url of endpoints) {
            const res = await fetch(url);
            const data = await res.json();
            if (data.results) allMovies = allMovies.concat(data.results);
        }

        // UNIQUE PAR ID
        const uniqueMovies = Array.from(new Map(allMovies.map(m => [m.id, m])).values());

        console.log(`🎬 ${uniqueMovies.length} films détectés`);

        const cache = loadCache();

        // FETCH DETAILS PARALLÈLE + CACHE
        const detailedMovies = await Promise.all(
            uniqueMovies.map(movie =>
                limit(async () => {
                    const cached = cache[movie.id];

                    if (isCacheValid(cached)) {
                        return cached.data;
                    }

                    const res = await fetch(`https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&append_to_response=release_dates`);
                    const data = await res.json();

                    cache[movie.id] = {
                        data,
                        timestamp: Date.now()
                    };

                    return data;
                })
            )
        );

        let finalResults = [];

        for (const details of detailedMovies) {
            if (!details.release_date) continue;

            const releaseCinema = new Date(details.release_date);

            const isFrench =
                details.production_countries?.some(c => c.iso_3166_1 === 'FR') ||
                details.original_language === 'fr';

            let vodDate = new Date(releaseCinema);
            vodDate.setDate(vodDate.getDate() + getDelay(details, isFrench));

            // PRIORITÉ DATE OFFICIELLE
            const digitalData = details.release_dates?.results
                ?.find(r => r.iso_3166_1 === 'FR' || r.iso_3166_1 === 'US')
                ?.release_dates.find(rd => rd.type === 4);

            if (digitalData) {
                const official = new Date(digitalData.release_date);
                if (!isNaN(official)) vodDate = official;
            }

            const diffDays = (vodDate - releaseCinema) / (1000 * 3600 * 24);

            if (
                vodDate >= startMonth &&
                vodDate <= endMonth &&
                diffDays > 20
            ) {
                finalResults.push({
                    title: details.title,
                    plex_release: formatDate(vodDate),
                    plex_release_timestamp: vodDate.getTime(),
                    tmdb_id: details.id,
                    poster_path: details.poster_path,
                    popularity: details.popularity || 0
                });
            }
        }

        // TRI
        finalResults.sort((a, b) => a.plex_release_timestamp - b.plex_release_timestamp);

        // DÉDUP SAFE
        const cleanResults = Array.from(
            new Map(finalResults.map(m => [m.tmdb_id, m])).values()
        );

        // SAVE
        fs.writeFileSync(DATA_PATH, JSON.stringify(cleanResults, null, 2), 'utf8');
        saveCache(cache);

        console.log(`✅ ${cleanResults.length} films prêts pour Plex`);

    } catch (err) {
        console.error("❌ Erreur :", err);
    }
}

updateVOD();
