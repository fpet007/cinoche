const fs = require('fs');
const path = require('path');

const TMDB_API_KEY = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';
const DATA_PATH = path.join(__dirname, '../data/plex-upcoming.json');

const STUDIOS = {
    UNIVERSAL: [33, 12248],
    WARNER: [174, 2734],
    DISNEY: [2, 420, 3, 1632, 6125],
    SONY: [5, 34],
    PARAMOUNT: [4, 60]
};

async function updateVOD() {
    try {
        const today = new Date();
        const currentMonth = today.getMonth();
        const currentYear = today.getFullYear();

        // On élargit le scan pour trouver les sorties US de février/mars qui deviennent VOD en Avril
        const dateDebutScan = new Date(); dateDebutScan.setMonth(today.getMonth() - 5);

        const endpoints = [
            // 1. SCAN FILMS FRANÇAIS (Sortis il y a 4 mois)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&with_origin_country=FR&primary_release_date.gte=${dateDebutScan.toISOString().split('T')[0]}&sort_by=popularity.desc&page=1`,
            // 2. SCAN BLOCKBUSTERS INTERNATIONAUX (Sortis il y a 45-60 jours)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=US&primary_release_date.gte=${dateDebutScan.toISOString().split('T')[0]}&sort_by=popularity.desc&page=1`
        ];

        let allMovies = [];
        for (const url of endpoints) {
            const res = await fetch(url);
            const data = await res.json();
            if (data.results) allMovies = allMovies.concat(data.results);
        }

        const uniqueMovies = Array.from(new Map(allMovies.map(m => [m.id, m])).values());
        let finalResults = [];

        for (const movie of uniqueMovies) {
            const detailRes = await fetch(`https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&append_to_response=release_dates`);
            const details = await detailRes.json();

            if (!details.release_date) continue;

            const releaseCinema = new Date(details.release_date);
            // On vérifie si c'est une pure production française (pour les 4 mois)
            const isPureFrench = details.production_countries?.some(c => c.iso_3166_1 === 'FR') && details.original_language === 'fr';
            const companyIds = details.production_companies?.map(c => c.id) || [];

            // --- CALCUL DU DÉLAI VOD (Option VFQ/US) ---
            let delay = 45; // Par défaut 45 jours (Sortie Digitale US/Canada)

            if (isPureFrench) {
                delay = 121; // On garde les 4 mois pour les films 100% FR (VFF)
            } else if (companyIds.some(id => STUDIOS.UNIVERSAL.includes(id))) {
                delay = 30; // Universal sort souvent ses VFQ en 30 jours
            } else if (companyIds.some(id => STUDIOS.WARNER.includes(id))) {
                delay = 45;
            } else if (companyIds.some(id => STUDIOS.DISNEY.includes(id))) {
                delay = 60;
            }

            let vodDate = new Date(releaseCinema);
            vodDate.setDate(vodDate.getDate() + delay);

            // Vérification si une date digitale officielle existe déjà (Type 4)
            const digitalData = details.release_dates?.results
                .find(r => r.iso_3166_1 === 'US' || r.iso_3166_1 === 'CA' || r.iso_3166_1 === 'FR')
                ?.release_dates.find(rd => rd.type === 4);

            if (digitalData) {
                const officialDate = new Date(digitalData.release_date);
                if (!isNaN(officialDate)) vodDate = officialDate;
            }

            // --- FILTRE MOIS EN COURS ---
            const isTargetMonth = (vodDate.getMonth() === currentMonth && vodDate.getFullYear() === currentYear);
            
            // Sécurité : On accepte le film si la VOD est au moins 20 jours après le ciné
            const diffDays = (vodDate - releaseCinema) / (1000 * 3600 * 24);

            if (isTargetMonth && diffDays >= 20) {
                finalResults.push({
                    title: movie.title,
                    plex_release: vodDate.toLocaleDateString('fr-FR', { day: 'numeric', month: 'long', year: 'numeric' }),
                    tmdb_id: movie.id,
                    poster_path: movie.poster_path,
                    _sort: vodDate.getTime()
                });
            }
        }

        finalResults.sort((a, b) => a._sort - b._sort);
        const cleanResults = Array.from(new Map(finalResults.map(m => [m.title, m])).values())
                                  .map(({_sort, ...rest}) => rest);

        fs.writeFileSync(DATA_PATH, JSON.stringify(cleanResults, null, 2), 'utf8');
        console.log(`✅ Scan terminé : ${cleanResults.length} films (Incluant VFQ/US).`);

    } catch (e) {
        console.error("Erreur :", e);
    }
}

updateVOD();
