const fs = require('fs');
const path = require('path');

const TMDB_API_KEY = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c'; // Clé TMDB
const BASE_URL = 'https://api.themoviedb.org/3';

// Studios IDs TMDB communs
const STUDIOS = {
    UNIVERSAL: [33],
    WARNER: [174, 2734],
    DISNEY: [2, 420, 3, 1632] // Walt Disney, Marvel, Pixar, Lucasfilm
};

const moisFrancais = ['Janvier', 'Février', 'Mars', 'Avril', 'Mai', 'Juin', 'Juillet', 'Août', 'Septembre', 'Octobre', 'Novembre', 'Décembre'];

function formatDateFR(dateObj) {
    return `${dateObj.getDate()} ${moisFrancais[dateObj.getMonth()]} ${dateObj.getFullYear()}`;
}

async function fetchTMDB(endpoint) {
    const res = await fetch(`${BASE_URL}${endpoint}&api_key=${TMDB_API_KEY}&language=fr-FR`);
    return res.json();
}

async function updatePlexVOD() {
    console.log("🎬 Début de l'analyse des prédictions VOD...");
    let upcomingVODs = [];

    try {
        // 1. Récupérer les films actuellement en salle (International et France)
        const nowPlaying = await fetchTMDB('/movie/now_playing?region=FR&page=1');
        const movies = nowPlaying.results;

        for (const movie of movies) {
            // 2. Récupérer les détails complets (pour avoir les studios et le pays d'origine)
            const details = await fetchTMDB(`/movie/${movie.id}?append_to_response=release_dates`);
            
            if (!details.release_date) continue;
            
            const releaseDate = new Date(details.release_date);
            let vodDelayDays = 45; // Base standard

            const isFrench = details.origin_country && details.origin_country.includes('FR');
            const companyIds = details.production_companies.map(c => c.id);

            // 3. APPLIQUER TA MÉTHODE MATHÉMATIQUE
            if (isFrench) {
                // Chronologie des médias FR (VOD achat = 4 mois)
                vodDelayDays = 120;
            } else if (companyIds.some(id => STUDIOS.UNIVERSAL.includes(id))) {
                vodDelayDays = 25; // Universal est très rapide
            } else if (companyIds.some(id => STUDIOS.WARNER.includes(id))) {
                vodDelayDays = 35; // Warner
            } else if (companyIds.some(id => STUDIOS.DISNEY.includes(id))) {
                vodDelayDays = 55; // Disney
            }

            // Calcul de la date estimée
            const predictedVODDate = new Date(releaseDate);
            predictedVODDate.setDate(predictedVODDate.getDate() + vodDelayDays);

            // On vérifie si on a une date OFFICIELLE numérique (Type 4 sur TMDB) pour corriger la prédiction
            let officialVOD = null;
            if (details.release_dates && details.release_dates.results) {
                const usReleases = details.release_dates.results.find(r => r.iso_3166_1 === 'US' || r.iso_3166_1 === 'FR');
                if (usReleases) {
                    const digitalRelease = usReleases.release_dates.find(r => r.type === 4);
                    if (digitalRelease) officialVOD = new Date(digitalRelease.release_date);
                }
            }

            const finalDate = officialVOD || predictedVODDate;
            const today = new Date();
            
            // On ne garde que les films dont la date VOD est dans le futur (ou sortie il y a moins de 3 jours)
            const diffTime = finalDate - today;
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));

            if (diffDays >= -3 && diffDays <= 120) {
                upcomingVODs.push({
                    title: movie.title,
                    plex_release: formatDateFR(finalDate),
                    tmdb_id: movie.id,
                    poster_path: movie.poster_path,
                    _sortDate: finalDate.getTime() // Pour le tri
                });
            }
        }

        // Trier par date de sortie la plus proche
        upcomingVODs.sort((a, b) => a._sortDate - b._sortDate);
        
        // Nettoyer l'objet avant la sauvegarde
        upcomingVODs = upcomingVODs.map(({_sortDate, ...rest}) => rest);

        // 4. Sauvegarder dans le fichier JSON
        const outputPath = path.join(__dirname, '../data/plex-upcoming.json');
        fs.writeFileSync(outputPath, JSON.stringify(upcomingVODs, null, 2));
        console.log(`✅ Succès ! ${upcomingVODs.length} films mis à jour dans plex-upcoming.json`);

    } catch (error) {
        console.error("❌ Erreur lors de la mise à jour :", error);
    }
}

updatePlexVOD();
