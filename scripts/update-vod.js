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

        // Fenêtres de recherche intelligentes
        // 1. Pour la France : films sortis au ciné il y a 3.5 à 5 mois
        const startFR = new Date(); startFR.setMonth(today.getMonth() - 5);
        const endFR = new Date(); endFR.setMonth(today.getMonth() - 3);
        
        // 2. Pour l'International : films sortis au ciné il y a 20 à 70 jours
        const startINT = new Date(); startINT.setDate(today.getDate() - 70);
        const endINT = new Date(); endINT.setDate(today.getDate() - 20);

        const queries = [
            // Scan France (Chronologie 4 mois)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&with_origin_country=FR&primary_release_date.gte=${startFR.toISOString().split('T')[0]}&primary_release_date.lte=${endFR.toISOString().split('T')[0]}&sort_by=primary_release_date.asc`,
            // Scan International (Fenêtre 45 jours)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=US&primary_release_date.gte=${startINT.toISOString().split('T')[0]}&primary_release_date.lte=${endINT.toISOString().split('T')[0]}&sort_by=popularity.desc`
        ];

        let allMovies = [];
        for (const url of queries) {
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
            const isFrench = details.production_countries?.some(c => c.iso_3166_1 === 'FR');
            const companyIds = details.production_companies?.map(c => c.id) || [];

            // --- CALCUL PRÉDICTIF ---
            let delay = 45; 
            if (isFrench) {
                delay = 121; // Loi française : 4 mois
            } else if (companyIds.some(id => STUDIOS.UNIVERSAL.includes(id))) {
                delay = 25;
            } else if (companyIds.some(id => STUDIOS.WARNER.includes(id))) {
                delay = 35;
            } else if (companyIds.some(id => STUDIOS.DISNEY.includes(id))) {
                delay = 60;
            }

            let vodDate = new Date(releaseCinema);
            vodDate.setDate(vodDate.getDate() + delay);

            // --- VÉRIFICATION DATE DIGITALE RÉELLE ---
            const releases = details.release_dates?.results || [];
            const digitalRelease = releases
                .find(r => r.iso_3166_1 === 'FR' || r.iso_3166_1 === 'US')
                ?.release_dates.find(rd => rd.type === 4);

            if (digitalRelease) {
                const officialDate = new Date(digitalRelease.release_date);
                if (!isNaN(officialDate)) vodDate = officialDate;
            }

            // --- FILTRES DE SÉCURITÉ CRITIQUES ---
            // 1. Uniquement le mois en cours
            const isSameMonth = vodDate.getMonth() === currentMonth && vodDate.getFullYear() === currentYear;
            
            // 2. SÉCURITÉ : La date VOD doit être AU MOINS 20 jours après la sortie Cinéma 
            // (C'est ce qui va bloquer "Vivaldi et moi" qui sort au ciné aujourd'hui)
            const diffDays = (vodDate - releaseCinema) / (1000 * 3600 * 24);
            const isNotCinemaRelease = diffDays > 20;

            if (isSameMonth && isNotCinemaRelease) {
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
        const cleanResults = finalResults.map(({_sort, ...rest}) => rest);

        fs.writeFileSync(DATA_PATH, JSON.stringify(cleanResults, null, 2), 'utf8');
        console.log(`✅ Succès : ${cleanResults.length} films VOD pour ce mois.`);

    } catch (e) {
        console.error("Erreur :", e);
    }
}

updateVOD();
