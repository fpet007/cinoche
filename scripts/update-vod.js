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

    // On cherche les films sortis au ciné depuis 6 mois pour couvrir toutes les fenêtres VOD
    const sixMonthsAgo = new Date();
    sixMonthsAgo.setMonth(today.getMonth() - 6);
    const dateStr = sixMonthsAgo.toISOString().split('T')[0];

    const url = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&sort_by=primary_release_date.desc&primary_release_date.gte=${dateStr}&include_video=false&page=1`;
    
    const response = await fetch(url);
    const data = await response.json();
    const movies = data.results || [];

    const upcomingList = [];

    for (const movie of movies) {
      const detailUrl = `https://api.themoviedb.org/3/movie/${movie.id}?api_key=${TMDB_API_KEY}&append_to_response=release_dates`;
      const detailRes = await fetch(detailUrl);
      const details = await detailRes.json();

      const countries = details.production_countries || [];
      const isFrench = countries.some(c => c.iso_3166_1 === 'FR');
      const releaseDate = new Date(details.release_date);

      if (isNaN(releaseDate)) continue;

      let vodDate = new Date(releaseDate);

      // Calcul prédictif
      if (isFrench) {
        vodDate.setDate(vodDate.getDate() + 121); // 4 mois
      } else {
        vodDate.setDate(vodDate.getDate() + 45);  // 45 jours
      }

      // Overwrite si une date digitale réelle existe
      const releaseResults = details.release_dates?.results || [];
      for (const country of releaseResults) {
        for (const release of country.release_dates) {
          if (release.type === 4) {
            const realVod = new Date(release.release_date);
            if (!isNaN(realVod)) vodDate = realVod;
          }
        }
      }

      // --- FILTRE : MOIS EN COURS OU MOIS PROCHAIN ---
      const isCurrentMonth = (vodDate.getMonth() === currentMonth && vodDate.getFullYear() === currentYear);
      const isNextMonth = (vodDate.getMonth() === nextMonth && vodDate.getFullYear() === (nextMonth === 0 ? currentYear + 1 : currentYear));

      if (isCurrentMonth || isNextMonth) {
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

    // Tri par date
    upcomingList.sort((a, b) => new Date(a.sort_date) - new Date(b.sort_date));

    // Sauvegarde
    const dir = path.dirname(DATA_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    
    fs.writeFileSync(DATA_PATH, JSON.stringify(upcomingList, null, 2), 'utf8');
    console.log(`✅ Succès : ${upcomingList.length} films trouvés (Avril & Mai).`);

  } catch (error) {
    console.error('❌ Erreur :', error);
    process.exit(1);
  }
}

updateVOD();
