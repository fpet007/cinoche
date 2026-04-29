const fs = require('fs');
const path = require('path');

const TMDB_API_KEY = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';
const DATA_PATH = path.join(__dirname, '../data/plex-upcoming.json');

const STUDIOS = {
    UNIVERSAL: [33, 12248],
    WARNER: [174, 2734],
    DISNEY: [2, 420, 3, 1632],
    SONY: [5, 34],
    PARAMOUNT: [4, 60]
};

async function updateVOD() {
    try {
        const today = new Date();
        const currentMonth = today.getMonth();
        const currentYear = today.getFullYear();

        // Scan large sur 5 mois pour attraper VFQ et VFF
        const dateDebutScan = new Date(); 
        dateDebutScan.setMonth(today.getMonth() - 5);
        const dateStr = dateDebutScan.toISOString().split('T')[0];

        const endpoints = [
            // 1. SCAN FRANCE (2 pages pour ne rien rater des films français)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&with_origin_country=FR&primary_release_date.gte=${dateStr}&sort_by=popularity.desc&page=1`,
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&with_origin_country=FR&primary_release_date.gte=${dateStr}&sort_by=popularity.desc&page=2`,
            // 2. SCAN INTERNATIONAL (Pour les blockbusters US/VFQ)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=US&primary_release_date.gte=${dateStr}&sort_by=popularity.desc&page=1`,
            // 3. SCAN GÉNÉRAL (Pour les films comme Mario ou Crime 101)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&primary_release_date.gte=${dateStr}&sort_by=popularity.desc&page=1`
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
            const isFrench = details.production_countries?.some(c => c.iso_3166_1 === 'FR') || details.original_language === 'fr';
            const companyIds = details.production_companies?.map(c => c.id) || [];

            // --- CALCUL DÉLAI OPTIMISÉ ---
            let delay = 45; // Standard VFQ
            
            if (isFrench) {
                delay = 121; // 4 mois VFF [cite: 14, 15]
            } else if (companyIds.some(id => STUDIOS.UNIVERSAL.includes(id))) {
                delay = 25; [cite: 15]
            } else if (companyIds.some(id => STUDIOS.WARNER.includes(id))) {
                delay = 35; [cite: 16]
            }

            let vodDate = new Date(releaseCinema);
            vodDate.setDate(vodDate.getDate() + delay);

            // Priorité date digitale réelle (Type 4) [cite: 19]
            const digitalData = details.release_dates?.results
                .find(r => r.iso_3166_1 === 'US' || r.iso_3166_1 === 'FR' || r.iso_3166_1 === 'CA')
                ?.release_dates.find(rd => rd.type === 4);

            if (digitalData) {
                const officialDate = new Date(digitalData.release_date);
                if (!isNaN(officialDate)) vodDate = officialDate; [cite: 21]
            }

            const isTargetMonth = (vodDate.getMonth() === currentMonth && vodDate.getFullYear() === currentYear); [cite: 21]
            const diffDays = (vodDate - releaseCinema) / (1000 * 3600 * 24); [cite: 22]

            // Sécurité minimale : 20 jours [cite: 25]
            if (isTargetMonth && diffDays >= 20) {
                finalResults.push({
                    title: movie.title,
                    plex_release: vodDate.toLocaleDateString('fr-FR', { day: 'numeric', month: 'long', year: 'numeric' }), [cite: 25]
                    tmdb_id: movie.id, [cite: 26]
                    poster_path: movie.poster_path, [cite: 26]
                    _sort: vodDate.getTime() [cite: 26]
                });
            }
        }

        finalResults.sort((a, b) => a._sort - b._sort); [cite: 27]
        const cleanResults = Array.from(new Map(finalResults.map(m => [m.title, m])).values()) [cite: 28]
                                  .map(({_sort, ...rest}) => rest); [cite: 28]

        fs.writeFileSync(DATA_PATH, JSON.stringify(cleanResults, null, 2), 'utf8'); [cite: 29]
        console.log(`✅ Mise à jour réussie : ${cleanResults.length} films.`); [cite: 29]

    } catch (e) {
        console.error("Erreur :", e); [cite: 30]
    }
}

updateVOD();
