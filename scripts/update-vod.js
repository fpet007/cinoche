const fs = require('fs');
const path = require('path');

const TMDB_API_KEY = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';
const DATA_PATH = path.join(__dirname, '../data/plex-upcoming.json');

const STUDIOS = {
    UNIVERSAL: [33],
    WARNER: [174, 2734],
    DISNEY: [2, 420, 3, 1632]
};

async function updateVOD() {
    try {
        const today = new Date();
        const currentMonth = today.getMonth();
        const currentYear = today.getFullYear();

        // On remonte 6 mois pour couvrir la chronologie des médias FR
        const sixMonthsAgo = new Date();
        sixMonthsAgo.setMonth(today.getMonth() - 6);
        const minDate = sixMonthsAgo.toISOString().split('T')[0];

        // --- MULTI-SCAN POUR NE RIEN RATER ---
        const fetchUrls = [
            // 1. Films populaires (Mondial)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&sort_by=popularity.desc&primary_release_date.gte=${minDate}&page=1`,
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&sort_by=popularity.desc&primary_release_date.gte=${minDate}&page=2`,
            // 2. Focus spécial PRODUCTIONS FRANÇAISES (Pour trouver Grand Ciel, Louise, etc.)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&with_origin_country=FR&primary_release_date.gte=${minDate}&sort_by=primary_release_date.desc&page=1`,
            // 3. Films actuellement ou récemment en salle
            `https://api.themoviedb.org/3/movie/now_playing?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR`
        ];

        let rawMovies = [];
        for (const url of fetchUrls) {
            const res = await fetch(url);
            const data = await res.json();
            if (data.results) rawMovies = rawMovies.concat(data.results);
        }

        // Suppression des doublons par ID
        const movies = Array.from(new Map(rawMovies.map(m => [m.id, m])).values());
        let finalResults = [];

        for (const movie of movies) {
            const detailRes = await fetch(`https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&append_to_response=release_dates`);
            const details = await detailRes.json();

            if (!details.release_date) continue;

            const releaseDate = new Date(details.release_date);
            const isFrench = details.production_countries?.some(c => c.iso_3166_1 === 'FR');
            const companyIds = details.production_companies?.map(c => c.id) || [];

            // --- CALCUL DE LA PRÉDICTION ---
            let delay = 45; // Standard International
            if (isFrench) {
                delay = 121; // Chronologie des médias FR (4 mois pile)
            } else if (companyIds.some(id => STUDIOS.UNIVERSAL.includes(id))) {
                delay = 25;
            } else if (companyIds.some(id => STUDIOS.WARNER.includes(id))) {
                delay = 35;
            } else if (companyIds.some(id => STUDIOS.DISNEY.includes(id))) {
                delay = 60;
            }

            let predictedDate = new Date(releaseDate);
            predictedDate.setDate(predictedDate.getDate() + delay);

            // --- RECHERCHE DE LA DATE RÉELLE (DIGITAL) ---
            const allReleases = details.release_dates?.results || [];
            
            // On cherche la date VOD en priorité en France (FR), puis aux USA (US)
            const targetedRegions = allReleases.filter(r => r.iso_3166_1 === 'FR' || r.iso_3166_1 === 'US');
            
            for (const region of targetedRegions) {
                const digital = region.release_dates.find(r => r.type === 4); // Type 4 = Digital
                if (digital) {
                    const realDate = new Date(digital.release_date);
                    // Si le film est français et qu'on trouve une date FR, ou si c'est US
                    if (!isNaN(realDate)) {
                        predictedDate = realDate;
                        break; 
                    }
                }
            }

            // --- FILTRE : MOIS EN COURS UNIQUEMENT ---
            if (predictedDate.getMonth() === currentMonth && predictedDate.getFullYear() === currentYear) {
                finalResults.push({
                    title: movie.title,
                    plex_release: predictedDate.toLocaleDateString('fr-FR', { day: 'numeric', month: 'long', year: 'numeric' }),
                    tmdb_id: movie.id,
                    poster_path: movie.poster_path,
                    _sort: predictedDate.getTime()
                });
            }
        }

        // Tri par date
        finalResults.sort((a, b) => a._sort - b._sort);
        const cleanResults = finalResults.map(({_sort, ...rest}) => rest);

        const dir = path.dirname(DATA_PATH);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(DATA_PATH, JSON.stringify(cleanResults, null, 2), 'utf8');

        console.log(`✅ Analyse terminée. ${cleanResults.length} films trouvés pour ce mois.`);

    } catch (e) {
        console.error("Erreur critique lors de l'update :", e);
    }
}

updateVOD();
