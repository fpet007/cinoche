const fs = require('fs');
const path = require('path');

const TMDB_API_KEY = process.env.TMDB_API_KEY;
const DATA_PATH = path.join(__dirname, '../data/plex-upcoming.json');

async function updateVOD() {
  try {
    const today = new Date();
    const currentMonth = today.getMonth(); // 0 = Janvier, 3 = Avril
    const currentYear = today.getFullYear();

    // 1. Chercher les films sortis récemment (6 derniers mois)
    const sixMonthsAgo = new Date();
    sixMonthsAgo.setMonth(today.getMonth() - 6);
    const dateStr = sixMonthsAgo.toISOString().split('T')[0];

    const url = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&sort_by=primary_release_date.desc&primary_release_date.gte=${dateStr}&include_video=false&page=1`;
    
    const response = await fetch(url);
    const data = await response.json();
    const movies = data.results || [];

    const upcomingList = [];

    for (const movie of movies) {
      // Chercher les détails pour le pays et les dates de sortie précises (Digital)
      const detailUrl = `https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&append_to_response=release_dates`;
      const detailRes = await fetch(detailUrl);
      const details = await detailRes.json();

      const countries = details.production_countries || [];
      const isFrench = countries.some(c => c.iso_3166_1 === 'FR');
      const releaseDate = new Date(details.release_date);

      if (isNaN(releaseDate)) continue;

      let vodDate = new Date(releaseDate);

      // --- LOGIQUE DE PRÉDICTION ---
      if (isFrench) {
        // France : Sortie Ciné + 4 mois (121 jours)
        vodDate.setDate(vodDate.getDate() + 121);
      } else {
        // USA/International : Sortie Ciné + 45 jours
        vodDate.setDate(vodDate.getDate() + 45);
      }

      // --- VÉRIFICATION SI DATE RÉELLE EXISTE ---
      const releaseResults = details.release_dates?.results || [];
      for (const country of releaseResults) {
        for (const release of country.release_dates) {
          if (release.type === 4) { // Type 4 = Digital/VOD
            const realVod = new Date(release.release_date);
            // Si une date officielle existe et qu'elle est différente, on la prend
            if (!isNaN(realVod)) vodDate = realVod;
          }
        }
      }

      // --- FILTRE STRICT DU MOIS EN COURS ---
      // On n'affiche que si le mois ET l'année correspondent à aujourd'hui
      if (vodDate.getMonth() === currentMonth && vodDate.getFullYear() === currentYear) {
        
        const options = { day: 'numeric', month: 'long', year: 'numeric' };
        upcomingList.push({
          title: movie.title,
          plex_release: vodDate.toLocaleDateString('fr-FR', options),
          tmdb_id: movie.id,
          poster_path: movie.poster_path,
          sort_date: vodDate.toISOString().split('T')[0]
        });
      }
    }

    // Trier les films par date (du plus proche au plus lointain)
    upcomingList.sort((a, b) => new Date(a.sort_date) - new Date(b.sort_date));

    // Sauvegarder le fichier
    const dir = path.dirname(DATA_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    
    fs.writeFileSync(DATA_PATH, JSON.stringify(upcomingList, null, 2), 'utf8');
    console.log(`✅ Succès : ${upcomingList.length} films trouvés pour ce mois.`);

  } catch (error) {
    console.error('❌ Erreur lors de la mise à jour :', error);
    process.exit(1);
  }
}

updateVOD();
