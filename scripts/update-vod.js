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

        console.log(`🔍 Scan ciblé : Sorties VOD de ${today.toLocaleDateString('fr-FR', {month: 'long', year: 'numeric'})}`);

        // Fenêtre pour la France (Cinéma + 4 mois) : on cherche les films sortis il y a ~120 jours
        const dateDebutFR = new Date(); dateDebutFR.setMonth(today.getMonth() - 5);
        const dateFinFR = new Date(); dateFinFR.setMonth(today.getMonth() - 3);
        
        // Fenêtre pour l'International (Cinéma + 45 jours) : on cherche les films sortis il y a ~2 mois
        const dateDebutINT = new Date(); dateDebutINT.setMonth(today.getMonth() - 3);

        const endpoints = [
            // 1. SCAN SPÉCIFIQUE FRANCE (On force la recherche sur les films français uniquement)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&with_origin_country=FR&primary_release_date.gte=${dateDebutFR.toISOString().split('T')[0]}&primary_release_date.lte=${dateFinFR.toISOString().split('T')[0]}&sort_by=primary_release_date.desc&page=1`,
            // 2. SCAN POPULARITÉ FRANCE (Pour les gros succès FR et sorties récentes)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=FR&primary_release_date.gte=${dateDebutFR.toISOString().split('T')[0]}&sort_by=popularity.desc&page=1`,
            // 3. SCAN INTERNATIONAL (Blockbusters US)
            `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&region=US&primary_release_date.gte=${dateDebutINT.toISOString().split('T')[0]}&sort_by=popularity.desc&page=1`
        ];

        let allMovies = [];
        for (const url of endpoints) {
            const res = await fetch(url);
            const data = await res.json();
            if (data.results) allMovies = allMovies.concat(data.results);
        }

        // Déduplication par ID
        const uniqueMovies = Array.from(new Map(allMovies.map(m => [m.id, m])).values());
        let finalResults = [];

        for (const movie of uniqueMovies) {
            const detailRes = await fetch(`https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&append_to_response=release_dates`);
            const details = await detailRes.json();

            if (!details.release_date) continue;

            const releaseCinema = new Date(details.release_date);
            const isFrench = details.production_countries?.some(c => c.iso_3166_1 === 'FR') || details.original_language === 'fr';
            const companyIds = details.production_companies?.map(c => c.id) || [];

            // --- CALCUL DE LA DATE VOD ---
            let delay = 45; 
            if (isFrench) {
                delay = 121; // Chronologie des médias FR : 4 mois
            } else if (companyIds.some(id => STUDIOS.UNIVERSAL.includes(id))) {
                delay = 28;
            } else if (companyIds.some(id => STUDIOS.WARNER.includes(id)) || companyIds.some(id => STUDIOS.SONY.includes(id))) {
                delay = 40;
            } else if (companyIds.some(id => STUDIOS.DISNEY.includes(id))) {
                delay = 60;
            }

            let vodDate = new Date(releaseCinema);
            vodDate.setDate(vodDate.getDate() + delay);

            // Vérification date officielle Digital (Type 4)
            const releaseResults = details.release_dates?.results || [];
            const digitalData = releaseResults.find(r => r.iso_3166_1 === 'FR' || r.iso_3166_1 === 'US')
                ?.release_dates.find(rd => rd.type === 4);

            if (digitalData) {
                const officialDate = new Date(digitalData.release_date);
                if (!isNaN(officialDate)) vodDate = officialDate;
            }

            // --- FILTRE STRICT : MOIS EN COURS ---
            const isTargetMonth = (vodDate.getMonth() === currentMonth && vodDate.getFullYear() === currentYear);
            
            // Sécurité pour éviter les erreurs de date de sortie cinéma
            const diffDays = (vodDate - releaseCinema) / (1000 * 3600 * 24);
            const securityCheck = isFrench ? diffDays > 115 : diffDays > 15;

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

        // Tri par date
        finalResults.sort((a, b) => a._sort - b._sort);
        
        // Nettoyage des doublons de titres
        const cleanResults = Array.from(new Map(finalResults.map(m => [m.title, m])).values())
                                  .map(({_sort, ...rest}) => rest);

        const dir = path.dirname(DATA_PATH);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(DATA_PATH, JSON.stringify(cleanResults, null, 2), 'utf8');

        console.log(`✅ Terminé : ${cleanResults.length} films pour ce mois.`);

    } catch (e) {
        console.error("Erreur critique :", e);
    }
}

updateVOD();
