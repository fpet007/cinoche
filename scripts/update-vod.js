const fs = require('fs');
const path = require('path');

const TMDB_API_KEY = process.env.TMDB_API_KEY;
const DATA_PATH = path.join(__dirname, '../data/plex-upcoming.json');

async function updateVOD() {
  try {
    const today = new Date();
    const currentMonth = today.getMonth();
    const nextMonth = (currentMonth + 1) % 12;
    const currentYear = today.getFullYear();

    // 1. On récupère deux listes : les nouveautés ET les films populaires
    const urls = [
      `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&sort_by=popularity.desc&primary_release_date.gte=${new Date(today.getFullYear() - 1, today.getMonth(), 1).toISOString().split('T')[0]}&include_video=false&page=1`,
      `https://api.themoviedb.org/3/movie/popular?api_key=${TMDB_API_KEY}&language=fr-FR&page=1`
    ];
    
    let allMovies = [];
    for (const url of urls) {
      const res = await fetch(url);
      const data = await res.json();
      if (data.results) allMovies = allMovies.concat(data.results);
    }

    // Supprimer les doublons par ID
    const uniqueMovies = Array.from(new Map(allMovies.map(m => [m.id, m])).values());
    const upcomingList = [];

    for (const movie of uniqueMovies) {
      const detailUrl = `https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&append_to_response=release_dates`;
      const detailRes = await fetch(detailUrl);
      const details = await detailRes.json();

      const countries = details.production_countries || [];
      const isFrench = countries.some(c => c.iso_3166_1 === 'FR');
      const releaseDate = new Date(details.release_date);

      if (isNaN(releaseDate)) continue;

      // Calcul prédictif par défaut
      let vodDate = new Date(releaseDate);
      if (isFrench) {
        vodDate.setDate(vodDate.getDate() + 121); // Chronologie France : 4 mois
      } else {
        vodDate.setDate(vodDate.getDate() + 45);  // Standard US : 45 jours
      }

      // Vérification d'une date VOD/Digital officielle dans TMDB
      const releaseResults = details.release_dates?.results || [];
      for (const country of releaseResults) {
        for (const release of country.release_dates) {
          if (release.type === 4 || release.type === 5) { // 4 = Digital, 5 = Physical
            const realVod = new Date(release.release_date);
            if (!isNaN(realVod)) {
                // On privilégie la date officielle si elle existe
                vodDate = realVod;
            }
          }
        }
      }

      // FILTRE : On accepte le mois en cours (Avril) et le mois suivant (Mai)
      const m = vodDate.getMonth();
      const y = vodDate.getFullYear();
      
      const isThisMonth = (m === currentMonth && y === currentYear);
      const isNextMonth = (m === nextMonth && (nextMonth === 0 ? y === currentYear + 1 : y === currentYear));

      if (isThisMonth || isNextMonth) {
        upcomingList.push({
          title: movie.title,
          plex_release: vodDate.toLocaleDateString('fr-FR', { day: 'numeric', month: 'long', year: 'numeric' }),
          tmdb_id: movie.id,
          poster_path: movie.poster_path,
          sort_date: vodDate.toISOString().split('T')[0]
        });
      }
    }

    // Tri chronologique
    upcomingList.sort((a, b) => new Date(a.sort_date) - new Date(b.sort_date));

    // Sauvegarde avec dossier auto-généré
    const dir = path.dirname(DATA_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    
    fs.writeFileSync(DATA_PATH, JSON.stringify(upcomingList, null, 2), 'utf8');
    console.log(`✅ Terminé : ${upcomingList.length} films enregistrés.`);

  } catch (error) {
    console.error('❌ Erreur critique :', error);
    process.exit(1);
  }
}

updateVOD();
