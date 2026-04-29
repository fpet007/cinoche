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

        console.log(`🔎 Début du scan ultra-profond pour le mois de ${today.toLocaleDateString('fr-FR', {month: 'long'})}...`);

        // On définit les fenêtres de sortie cinéma pour trouver la VOD de MAINTENANT
        const fourMonthsAgo = new Date(); fourMonthsAgo.setMonth(today.getMonth() - 5);
        const twoMonthsAgo = new Date(); twoMonthsAgo.setMonth(today.getMonth() - 1);

        let allMovies = [];

        // --- SCAN 1 : LES PRODUCTIONS FRANÇAISES (5 pages pour ne rien rater) ---
        for (let page = 1; page <= 5; page++) {
            const url = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&with_original_language=fr&primary_release_date.gte=${fourMonthsAgo.toISOString().split('T')[0]}&primary_release_date.lte=${twoMonthsAgo.toISOString().split('T')[0]}&sort_by=primary_release_date.desc&page=${page}`;
            const res = await fetch(url);
            const data = await res.json();
            if (data.results) allMovies = allMovies.concat(data.results);
        }

        // --- SCAN 2 : LES BLOCKBUSTERS INTERNATIONAUX (2 pages) ---
        for (let page = 1; page <= 2; page++) {
            const url = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&primary_release_date.gte=${twoMonthsAgo.toISOString().split('T')[0]}&sort_by=popularity.desc&page=${page}`;
            const res = await fetch(url);
            const data = await res.json();
            if (data.results) allMovies = allMovies.concat(data.results);
        }

        // Suppression des doublons
        const movies = Array.from(new Map(allMovies.map(m => [m.id, m])).values());
        let finalResults = [];

        for (const movie of movies) {
            const detailRes = await fetch(`https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&append_to_response=release_dates`);
            const details = await detailRes.json();

            if (!details.release_date) continue;

            const releaseCinema = new Date(details.release_date);
            const isFrench = details.production_countries?.some(c => c.iso_3166_1 === 'FR') || details.original_language === 'fr';
            const companyIds = details.production_companies?.map(c => c.id) || [];

            // --- CALCUL PRÉDICTIF SELON TES RÈGLES ---
            let delay = 45; 
            if (isFrench) {
                delay = 121; // Chronologie FR (4 mois)
            } else if (companyIds.some(id => STUDIOS.UNIVERSAL.includes(id))) {
                delay = 25;
            } else if (companyIds.some(id => STUDIOS.WARNER.includes(id))) {
                delay = 35;
            } else if (companyIds.some(id => STUDIOS.DISNEY.includes(id))) {
                delay = 60;
            }

            let vodDate = new Date(releaseCinema);
            vodDate.setDate(vodDate.getDate() + delay);

            // Vérification si date Digital officielle
            const releases = details.release_dates?.results || [];
            const digitalRelease = releases
                .find(r => r.iso_3166_1 === 'FR' || r.iso_3166_1 === 'US')
                ?.release_dates.find(rd => rd.type === 4);

            if (digitalRelease) {
                const officialDate = new Date(digitalRelease.release_date);
                if (!isNaN(officialDate)) vodDate = officialDate;
            }

            // --- FILTRES DE PRÉCISION ---
            const isSameMonth = vodDate.getMonth() === currentMonth && vodDate.getFullYear() === currentYear;
            
            // Sécurité : On exclut les films qui sortent au ciné en même temps (diff < 20 jours)
            const diffDays = (vodDate - releaseCinema) / (1000 * 3600 * 24);

            if (isSameMonth && diffDays > 20) {
                finalResults.push({
                    title: movie.title,
                    plex_release: vodDate.toLocaleDateString('fr-FR', { day: 'numeric', month: 'long', year: 'numeric' }),
                    tmdb_id: movie.id,
                    poster_path: movie.poster_path,
                    _sort: vodDate.getTime()
                });
            }
        }

        // Tri et sauvegarde
        finalResults.sort((a, b) => a._sort - b._sort);
        const cleanResults = Array.from(new Map(finalResults.map(m => [m.tmdb_id, m])).values()) // Anti-doublon final
                                  .map(({_sort, ...rest}) => rest);

        fs.writeFileSync(DATA_PATH, JSON.stringify(cleanResults, null, 2), 'utf8');
        console.log(`✅ Scan terminé : ${cleanResults.length} films trouvés pour ce mois.`);

    } catch (e) {
        console.error("Erreur :", e);
    }
}

updateVOD();
