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

        console.log(`🚀 Scan exhaustif : Nouveautés VOD d'Avril 2026...`);

        // Fenêtre large pour capturer les sorties ciné qui deviennent VOD ce mois-ci
        const searchStart = new Date(); searchStart.setMonth(today.getMonth() - 6);
        const dateLimit = searchStart.toISOString().split('T')[0];

        let allMovies = [];
        
        // SCAN MASSIF : On demande 10 pages de résultats pour être sûr de trouver les films FR
        for (let i = 1; i <= 10; i++) {
            const url = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&primary_release_date.gte=${dateLimit}&sort_by=popularity.desc&page=${i}`;
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

            // --- CALCUL PRÉDICTIF ---
            let delay = 45; // Standard international
            if (isFrench) {
                delay = 121; // Loi française stricte (4 mois)
            } else if (companyIds.some(id => STUDIOS.UNIVERSAL.includes(id))) {
                delay = 28;
            } else if (companyIds.some(id => STUDIOS.WARNER.includes(id)) || companyIds.some(id => STUDIOS.SONY.includes(id))) {
                delay = 40;
            } else if (companyIds.some(id => STUDIOS.DISNEY.includes(id))) {
                delay = 60;
            }

            let vodDate = new Date(releaseCinema);
            vodDate.setDate(vodDate.getDate() + delay);

            // --- VÉRIFICATION DATE OFFICIELLE (TYPE 4 = DIGITAL) ---
            const releaseResults = details.release_dates?.results || [];
            // On cherche une date digitale officielle en priorité
            const digitalData = releaseResults.find(r => r.iso_3166_1 === 'FR' || r.iso_3166_1 === 'US')
                ?.release_dates.find(rd => rd.type === 4);

            if (digitalData) {
                const officialDate = new Date(digitalData.release_date);
                if (!isNaN(officialDate)) vodDate = officialDate;
            }

            // --- FILTRES DE PRÉCISION ---
            // 1. Uniquement le mois et l'année en cours (Avril 2026)
            const isTargetMonth = (vodDate.getMonth() === currentMonth && vodDate.getFullYear() === currentYear);
            
            // 2. Sécurité : La VOD ne peut pas sortir avant le cinéma (bug Mission Rosetta/Vivaldi)
            // On impose un minimum de 15 jours après le ciné pour les US et 120 pour les FR
            const diffDays = (vodDate - releaseCinema) / (1000 * 3600 * 24);
            const securityCheck = isFrench ? diffDays > 110 : diffDays > 15;

            if (isTargetMonth && securityCheck) {
                finalResults.push({
                    title: movie.title,
                    plex_release: vodDate.toLocaleDateString('fr-FR', { day: 'numeric', month: 'long', year: 'numeric' }),
                    tmdb_id: movie.id,
                    poster_path: movie.poster_path,
                    _sort: vodDate.getTime()
                });
            }
        }

        // Tri chronologique (du 1er au 30 avril)
        finalResults.sort((a, b) => a._sort - b._sort);
        
        // Suppression des doublons de titre (fréquent sur TMDB)
        const cleanResults = Array.from(new Map(finalResults.map(m => [m.title, m])).values())
                                  .map(({_sort, ...rest}) => rest);

        fs.writeFileSync(DATA_PATH, JSON.stringify(cleanResults, null, 2), 'utf8');
        console.log(`✅ Succès ! ${cleanResults.length} nouveautés VOD trouvées pour ce mois.`);

    } catch (e) {
        console.error("Erreur critique :", e);
    }
}

updateVOD();
