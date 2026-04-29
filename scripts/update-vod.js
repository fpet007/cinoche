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
        const currentMonth = today.getMonth(); // Mois actuel (0-11)
        const currentYear = today.getFullYear();

        console.log(`Analyse des sorties pour : ${today.toLocaleDateString('fr-FR', {month: 'long', year: 'numeric'})}`);

        // On cherche large : tous les films sortis depuis 6 mois
        const sixMonthsAgo = new Date();
        sixMonthsAgo.setMonth(today.getMonth() - 6);
        const minDate = sixMonthsAgo.toISOString().split('T')[0];

        const url = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&sort_by=popularity.desc&primary_release_date.gte=${minDate}&include_video=false&page=1`;
        
        const res = await fetch(url);
        const data = await res.json();
        const movies = data.results || [];

        let finalResults = [];

        for (const movie of movies) {
            // Détails pour avoir les studios et dates précises
            const detailRes = await fetch(`https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&append_to_response=release_dates`);
            const details = await detailRes.json();

            if (!details.release_date) continue;

            const releaseDate = new Date(details.release_date);
            const isFrench = details.production_countries?.some(c => c.iso_3166_1 === 'FR');
            const companyIds = details.production_companies?.map(c => c.id) || [];

            // --- TA LOGIQUE MATHÉMATIQUE ---
            let delay = 45; // Base
            if (isFrench) {
                delay = 121; // 4 mois pile
            } else if (companyIds.some(id => STUDIOS.UNIVERSAL.includes(id))) {
                delay = 25;
            } else if (companyIds.some(id => STUDIOS.WARNER.includes(id))) {
                delay = 35;
            } else if (companyIds.some(id => STUDIOS.DISNEY.includes(id))) {
                delay = 55;
            }

            let predictedDate = new Date(releaseDate);
            predictedDate.setDate(predictedDate.getDate() + delay);

            // --- VÉRIFICATION DATE OFFICIELLE (DIGITAL) ---
            // On cherche si une date VOD réelle existe pour écraser la prédiction
            const allReleases = details.release_dates?.results || [];
            const targetedReleases = allReleases.find(r => r.iso_3166_1 === 'US' || r.iso_3166_1 === 'FR');
            
            if (targetedReleases) {
                const digital = targetedReleases.release_dates.find(r => r.type === 4);
                if (digital) {
                    predictedDate = new Date(digital.release_date);
                }
            }

            // --- FILTRE STRICT : MOIS EN COURS UNIQUEMENT ---
            // C'est ici que la magie opère.
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

        // Sauvegarde
        const dir = path.dirname(DATA_PATH);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(DATA_PATH, JSON.stringify(cleanResults, null, 2), 'utf8');

        console.log(`✅ Terminé ! ${cleanResults.length} films pour ce mois.`);

    } catch (e) {
        console.error("Erreur:", e);
    }
}

updateVOD();
