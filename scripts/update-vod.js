/**
 * updateVOD.js — v7
 * =================
 * Génère la liste des FILMS DE CINÉMA en VOD pour le mois en cours STRICT.
 * Plex FR — uniquement de vrais longs-métrages sortis en salle.
 *
 * Nouveautés v7 :
 * ✅ Scraping MaxBlizz.com pour récupérer les dates VOD US officielles
 *    (source plus fiable que la prédiction TMDB, souvent annoncée 2-3 sem. avant la sortie)
 * ✅ Snap to Tuesday : ta règle empirique « les blockbusters US sortent le mardi »
 *    appliquée aux dates PRÉDITES pour films US uniquement (les officielles restent intactes).
 * ✅ Système d'overrides manuels (MANUAL_OVERRIDES) pour cas connus / corrections perso.
 * ✅ Hiérarchie des sources : override > maxblizz > officielle TMDB > prédite (snap mardi US).
 * ✅ Cache séparé pour le scraping MaxBlizz (12h TTL).
 * ✅ Matching par titre intelligent (normalisation, accents, sous-titres).
 *
 * Hérité de v6 (conservé) :
 * ✅ Mois strict (pas de lookahead).
 * ✅ Filtre anti-fantôme/anti-théâtre composite (vote_count >= 5 OU popularity >= 1.5).
 * ✅ Mots-clés enrichis anti-captation théâtre/spectacle.
 *
 * Usage  : node updateVOD.js
 * Cron   : 0 2 * * * (tous les soirs à 2h)
 */

'use strict';
const fs   = require('fs');
const path = require('path');

// ─── CONFIG ────────────────────────────────────────────────────────────────────

const TMDB_API_KEY = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';
const DATA_PATH        = path.join(__dirname, '../data/plex-upcoming.json');
const CACHE_PATH       = path.join(__dirname, '../data/.tmdb-cache.json');
const MAXBLIZZ_CACHE   = path.join(__dirname, '../data/.maxblizz-cache.json');

const DELAYS = {
  FRENCH  : 120,   // Chronologie des médias FR — VOD à l'acte
  AMERICAN:  45,   // PVOD/TVOD international standard
};

// Cache TMDB
const CACHE_TTL_HOURS = 24;
// Cache MaxBlizz (plus court : on veut capter les nouvelles annonces)
const MAXBLIZZ_TTL_HOURS = 12;
// Délai entre appels (TMDB tolère ~50 req/s ; on reste très conservateur)
const API_DELAY_MS = 130;
// Pages max par endpoint discover
const MAX_PAGES_PER_ENDPOINT = 6;
// Durée minimum pour ne pas être un court-métrage (minutes)
const MIN_RUNTIME = 40;

// Genres TMDB exclus : Documentaire (99), Musique (10402), Téléfilm (10770)
const EXCLUDED_GENRE_IDS = new Set([99, 10402, 10770]);

/**
 * Mots-clés titres qui trahissent une captation de spectacle vivant ou théâtre.
 */
const SPECTACLE_KEYWORDS = [
  'symphony', 'symphonie', 'philharmonic', 'philharmonique',
  ' opera', 'opera:', "l'opéra", 'opéra de paris', 'paris opera',
  'in concert', 'live at', 'live in', 'live from', 'en concert',
  ' tour ', 'world tour', 'la tournée',
  ' ballet', 'casse-noisette', 'nutcracker', 'der nussknacker',
  'récital', 'recital', 'metropolitan opera', 'royal opera',
  'gaming x symphony',
  'théâtre', 'theatre', 'pièce de', 'comédie française', 'captation',
  'molière', 'charbon dans les veines',
];

/**
 * Mots-clés titres qui trahissent un téléfilm policier régional FR.
 */
const TELEFILM_TITLE_PATTERNS = [
  /^meurtres? à /i,
  /^crimes? à /i,
  /^disparition à /i,
  /^enquêtes? à /i,
  /^un (mystère|crime) à /i,
];

/**
 * ─── OVERRIDES MANUELS ──────────────────────────────────────────────────────
 * Source de vérité prioritaire. Format : { tmdbId|titleNorm: { date: 'YYYY-MM-DD', reason } }
 * Utilisé pour les films à forte attente où tu as une info plus fiable que TMDB/MaxBlizz.
 * Le titre normalisé sert de fallback si tu n'as pas le tmdb_id sous la main.
 */
const MANUAL_OVERRIDES = {
  // Exemples — à éditer / ajouter selon les besoins
  // 'project hail mary' : { date: '2026-05-12', reason: 'film à fort potentiel, repoussé' },
  // 698687               : { date: '2026-05-19', reason: 'TMDB ID prioritaire' },
};

// ─── HELPERS DE BASE ───────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchWithRetry(url, retries = 3, opts = {}) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, opts);
      if (res.status === 429) {
        await sleep(2000 * attempt);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return opts.text ? await res.text() : await res.json();
    } catch (err) {
      if (attempt === retries) throw err;
      await sleep(attempt * 600);
    }
  }
}

async function fetchAllPages(baseUrl, maxPages = MAX_PAGES_PER_ENDPOINT) {
  const movies = [];
  for (let page = 1; page <= maxPages; page++) {
    const data = await fetchWithRetry(`${baseUrl}&page=${page}`);
    if (!data?.results?.length) break;
    movies.push(...data.results);
    if (page >= (data.total_pages || 1)) break;
    await sleep(API_DELAY_MS);
  }
  return movies;
}

// ─── NORMALISATION TITRES (pour matching MaxBlizz / overrides) ────────────────

function normalizeTitle(s) {
  if (!s) return '';
  return s
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')   // retire les accents
    .replace(/['']/g, '')                                // retire apostrophes
    .replace(/[^a-z0-9]+/g, ' ')                         // ponctuation → espace
    .trim()
    .replace(/^the |^le |^la |^les |^l /, '');           // articles d'attaque
}

// ─── CACHE TMDB ────────────────────────────────────────────────────────────────

function loadCache(cachePath = CACHE_PATH) {
  try {
    const raw = fs.readFileSync(cachePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

function saveCache(cache, cachePath = CACHE_PATH) {
  try {
    fs.mkdirSync(path.dirname(cachePath), { recursive: true });
    const tmp = cachePath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(cache), 'utf8');
    fs.renameSync(tmp, cachePath);
  } catch (err) {
    console.warn(`  ⚠️  Cache non sauvegardé : ${err.message}`);
  }
}

function isCacheEntryFresh(entry, ttlHours = CACHE_TTL_HOURS) {
  if (!entry?._cachedAt) return false;
  const ageHours = (Date.now() - entry._cachedAt) / 3600000;
  return ageHours < ttlHours;
}

// ─── SCRAPING MAXBLIZZ ────────────────────────────────────────────────────────

/**
 * Récupère les annonces VOD de maxblizz.com et extrait :
 *  - le titre du film
 *  - la date VOD US officielle annoncée
 * Stratégie : on lit la page liste, on suit chaque article récent, on parse la date.
 *
 * Retourne un Map<titleNormalized, { date: Date, originalTitle: string, url: string }>
 */
async function fetchMaxblizzReleases() {
  const cache = loadCache(MAXBLIZZ_CACHE);
  if (cache._cachedAt && isCacheEntryFresh(cache, MAXBLIZZ_TTL_HOURS)) {
    console.log(`  💾 MaxBlizz : cache valide (${Object.keys(cache.releases || {}).length} entrées)`);
    return new Map(Object.entries(cache.releases || {}).map(([k, v]) => [k, {
      ...v, date: new Date(v.date),
    }]));
  }

  console.log(`  🌐 MaxBlizz : scraping en cours...`);
  const releases = new Map();

  try {
    // 1. Page liste : on récupère les liens vers les articles individuels
    const listHtml = await fetchWithRetry(
      'https://maxblizz.com/dvd-and-vod-release-dates/',
      3,
      { text: true, headers: { 'User-Agent': 'Mozilla/5.0 updateVOD-bot' } }
    );

    // Extrait les liens d'articles : pattern <a href="https://maxblizz.com/[slug]-vod-release-date-revealed/">
    const articleRegex = /href="(https:\/\/maxblizz\.com\/[a-z0-9-]+-vod-release-date-revealed\/?)"/gi;
    const articleUrls = new Set();
    let match;
    while ((match = articleRegex.exec(listHtml)) !== null) {
      articleUrls.add(match[1]);
    }

    console.log(`     ${articleUrls.size} articles VOD trouvés`);

    // 2. Pour chaque article, on extrait le titre du film + la date VOD US annoncée
    let parsed = 0;
    for (const url of articleUrls) {
      try {
        await sleep(250); // soft rate limit pour ne pas matraquer le site
        const html = await fetchWithRetry(url, 2, {
          text: true, headers: { 'User-Agent': 'Mozilla/5.0 updateVOD-bot' },
        });

        // Cherche une date au format "Month DD, YYYY" dans le contenu
        // Plusieurs patterns possibles : "starting May 5, 2026", "on March 31, 2026", etc.
        const dateRegex = /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})\b/gi;
        const dates = [];
        let dm;
        while ((dm = dateRegex.exec(html)) !== null) {
          const d = new Date(`${dm[1]} ${dm[2]}, ${dm[3]} 12:00:00 UTC`);
          if (!isNaN(d)) dates.push(d);
        }

        // On prend la date la plus probable : la plus future (date de SORTIE, pas date de l'article)
        // Heuristique : on filtre les dates < aujourd'hui - 7j (ce sont des dates d'articles)
        const cutoff = Date.now() - 7 * 86400000;
        const futureDates = dates.filter((d) => d.getTime() > cutoff);
        const vodDate = futureDates.length
          ? futureDates.sort((a, b) => a - b)[0]   // la plus proche dans le futur
          : null;

        if (!vodDate) continue;

        // Extrait le titre du film depuis le slug : 
        //   .../the-super-mario-galaxy-movie-vod-release-date-revealed/
        //     → "the super mario galaxy movie"
        const slugMatch = url.match(/maxblizz\.com\/([a-z0-9-]+)-vod-release-date-revealed/i);
        if (!slugMatch) continue;
        const rawSlug = slugMatch[1].replace(/-/g, ' ');

        // Nettoie : retire des préfixes redondants type "lee cronins", "sam raimis", "maggie gyllenhaals"
        // (le scraping liste l'a montré : ces noms de réalisateur polluent le slug)
        const cleanedTitle = rawSlug
          .replace(/^[a-z]+ ?[a-z]+s? /, (m) => {
            // Si le préfixe ressemble à un nom propre possessif, on l'enlève seulement s'il y a >=3 mots après
            const rest = rawSlug.slice(m.length).trim();
            return rest.split(' ').length >= 2 ? '' : m;
          })
          .trim();

        const norm = normalizeTitle(cleanedTitle);
        if (!norm) continue;

        // En cas de collision, on garde la date la plus précoce (= annonce la plus ferme)
        const existing = releases.get(norm);
        if (!existing || vodDate < existing.date) {
          releases.set(norm, {
            date: vodDate,
            originalTitle: cleanedTitle,
            url,
          });
        }
        parsed++;
      } catch (err) {
        // article inaccessible → on passe
      }
    }

    console.log(`     ✓ ${parsed} dates VOD US extraites de MaxBlizz`);

    // Cache
    const toCache = {
      _cachedAt: Date.now(),
      releases: Object.fromEntries(
        Array.from(releases.entries()).map(([k, v]) => [k, { ...v, date: v.date.toISOString() }])
      ),
    };
    saveCache(toCache, MAXBLIZZ_CACHE);
  } catch (err) {
    console.warn(`  ⚠️  MaxBlizz scraping a échoué : ${err.message} — on continue sans.`);
  }

  return releases;
}

/**
 * Cherche un film dans la map MaxBlizz par titre (FR ou original).
 * Matching tolérant : exact, préfixe, ou contenu mutuel.
 */
function findMaxblizzMatch(maxblizzMap, title, originalTitle) {
  const candidates = [title, originalTitle].filter(Boolean).map(normalizeTitle);
  for (const c of candidates) {
    if (!c) continue;
    if (maxblizzMap.has(c)) return maxblizzMap.get(c);
    // Match tolérant : titre TMDB inclus dans une clé MaxBlizz, ou inversement.
    // Garde-fou : on exige >= 8 caractères pour éviter les faux positifs ("the", "ana"…).
    if (c.length < 8) continue;
    for (const [k, v] of maxblizzMap.entries()) {
      if (k.length < 8) continue;
      if (k === c) return v;
      if (k.includes(c) || c.includes(k)) {
        // Vérifie que ce n'est pas un match trop large
        const ratio = Math.min(k.length, c.length) / Math.max(k.length, c.length);
        if (ratio >= 0.6) return v;
      }
    }
  }
  return null;
}

// ─── HELPERS MÉTIER ────────────────────────────────────────────────────────────

function isFrenchProduction(details) {
  const hasFRCountry  = details.production_countries?.some((c) => c.iso_3166_1 === 'FR') ?? false;
  const hasFROrigin   = details.origin_country?.includes('FR') ?? false;
  const isFrLang      = details.original_language === 'fr';
  const score = (hasFRCountry ? 1 : 0) + (hasFROrigin ? 1 : 0) + (isFrLang ? 1 : 0);
  return score >= 2;
}

function hasExcludedGenre(details) {
  return details.genres?.some((g) => EXCLUDED_GENRE_IDS.has(g.id)) ?? false;
}

function isSpectacle(details) {
  const titles = [details.title, details.original_title]
    .filter(Boolean)
    .map((t) => t.toLowerCase());
  return titles.some((t) => SPECTACLE_KEYWORDS.some((kw) => t.includes(kw)));
}

function isTelefilmByTitle(details) {
  const title = details.title || '';
  return TELEFILM_TITLE_PATTERNS.some((rx) => rx.test(title));
}

function isGhostEntry(details) {
  const noGenres   = !details.genres || details.genres.length === 0;
  const noVotes    = (details.vote_count ?? 0) === 0 && (details.vote_average ?? 0) === 0;
  const noPoster   = !details.poster_path;
  const badTitle   = /^untitled$|^sans titre$|^\s*$/i.test(details.title || '');
  const score = (noGenres ? 1 : 0) + (noVotes ? 1 : 0) + (noPoster ? 1 : 0) + (badTitle ? 2 : 0);
  return score >= 2;
}

function isTooShort(details) {
  const runtime = details.runtime;
  if (!runtime || runtime === 0) return false;
  return runtime < MIN_RUNTIME;
}

function hasTheatricalReleaseFR(releaseDates) {
  const fr = releaseDates?.results?.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return false;
  return fr.release_dates.some((rd) => rd.type === 3 || rd.type === 2);
}

function getTheatricalDateFR(releaseDates) {
  const fr = releaseDates?.results?.find((r) => r.iso_3166_1 === 'FR');
  if (!fr) return null;
  const theatrical = fr.release_dates
    .filter((rd) => rd.type === 3 || rd.type === 2)
    .map((rd) => new Date(rd.release_date))
    .filter((d) => !isNaN(d))
    .sort((a, b) => a - b)[0];
  return theatrical ?? null;
}

function getOfficialDigitalDate(releaseDates) {
  for (const region of ['FR', 'US']) {
    const entry = releaseDates?.results?.find((r) => r.iso_3166_1 === region);
    if (!entry) continue;
    const digital = entry.release_dates
      .filter((rd) => rd.type === 4)
      .map((rd) => new Date(rd.release_date))
      .filter((d) => !isNaN(d))
      .sort((a, b) => a - b)[0];
    if (digital) return { date: digital, region };
  }
  return null;
}

function predictVODDate(cinemaDate, isFrench) {
  const vod = new Date(cinemaDate);
  vod.setDate(vod.getDate() + (isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN));
  return vod;
}

/**
 * Snap to Tuesday (règle empirique de l'utilisateur) : pour les films US uniquement,
 * on aligne la date prédite sur le PROCHAIN mardi (jamais en arrière).
 * Les blockbusters américains sortent en VOD le mardi (PVOD/TVOD US).
 * On NE TOUCHE PAS aux dates officielles ni MaxBlizz : ces sources sont déjà précises.
 *
 * Important : on n'arrondit JAMAIS en arrière, sinon on passerait sous le délai
 * minimum chronologie médias (45j US) et le film serait éjecté du résultat.
 * Au pire on ajoute 0..6 jours, jamais on n'en retire.
 */
function snapToTuesday(date) {
  const d = new Date(date);
  const day = d.getDay(); // 0=dim, 1=lun, 2=mar, 3=mer, 4=jeu, 5=ven, 6=sam
  // Décalage vers le prochain mardi (mardi=2). Si on est déjà mardi, on reste.
  // dim=0 → +2, lun=1 → +1, mar=2 → 0, mer=3 → +6, jeu=4 → +5, ven=5 → +4, sam=6 → +3
  const delta = (2 - day + 7) % 7;
  d.setDate(d.getDate() + delta);
  return d;
}

function formatDateFR(date) {
  return date.toLocaleDateString('fr-FR', {
    day: 'numeric', month: 'long', year: 'numeric',
  });
}

function ymd(date) {
  return date.toISOString().split('T')[0];
}

// ─── FENÊTRES & ENDPOINTS ─────────────────────────────────────────────────────

/**
 * v6: Mois strictement en cours, aucune anticipation sur le mois suivant.
 */
function computeTargetWindow(now = new Date()) {
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const monthEnd   = new Date(now.getFullYear(), now.getMonth() + 1, 0, 23, 59, 59);
  return { monthStart, monthEnd, windowEnd: monthEnd };
}

function buildScanEndpoints(monthStart, windowEnd) {
  const frStart = new Date(windowEnd);
  frStart.setMonth(frStart.getMonth() - 6); frStart.setDate(frStart.getDate() - 15);
  const frEnd   = new Date(monthStart); frEnd.setDate(frEnd.getDate() - 100);

  const intlStart = new Date(windowEnd);
  intlStart.setMonth(intlStart.getMonth() - 4);
  const intlEnd   = new Date(monthStart); intlEnd.setDate(intlEnd.getDate() - 30);

  const base = `https://api.themoviedb.org/3/discover/movie?api_key=${TMDB_API_KEY}&language=fr-FR&include_adult=false`;
  return [
    { name: 'FR/origin/popular',
      url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },
    { name: 'FR/origin/quality',
      url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=vote_average.desc&vote_count.gte=10` },
    { name: 'FR/lang+region',
      url: `${base}&with_original_language=fr&region=FR&with_release_type=2|3&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },
    { name: 'FR/origin/recent',
      url: `${base}&with_origin_country=FR&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=primary_release_date.desc` },
    { name: 'FR/theatrical-net',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(frStart)}&primary_release_date.lte=${ymd(frEnd)}&sort_by=popularity.desc` },
    { name: 'INTL/popular',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=popularity.desc` },
    { name: 'INTL/quality',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=vote_average.desc&vote_count.gte=40` },
    { name: 'INTL/fresh',
      url: `${base}&region=FR&with_release_type=3&primary_release_date.gte=${ymd(intlStart)}&primary_release_date.lte=${ymd(intlEnd)}&sort_by=primary_release_date.desc` },
  ];
}

// ─── DÉTAILS FILM (avec cache) ────────────────────────────────────────────────

async function fetchMovieDetails(movieId, cache) {
  const key = String(movieId);
  if (cache[key] && isCacheEntryFresh(cache[key])) {
    return cache[key].data;
  }
  const url = `https://api.themoviedb.org/3/movie/${movieId}?api_key=${TMDB_API_KEY}&language=fr-FR&append_to_response=release_dates`;
  const data = await fetchWithRetry(url);
  cache[key] = { _cachedAt: Date.now(), data };
  return data;
}

// ─── RÉSOLUTION DE LA DATE VOD ────────────────────────────────────────────────

/**
 * Détermine la date VOD finale d'un film selon la hiérarchie :
 *   1. Override manuel (par tmdb_id ou titre normalisé)
 *   2. MaxBlizz (date US annoncée)
 *   3. Date numérique officielle TMDB (FR puis US)
 *   4. Prédiction (cinemaDate + délai), avec snap to Tuesday pour les films US
 */
function resolveVODDate({ details, movie, cinemaDate, isFrench, maxblizzMap, windowEnd }) {
  // 1. Override manuel
  const titleNorm = normalizeTitle(details.title || movie.title);
  const origNorm  = normalizeTitle(details.original_title || '');
  const overrideById   = MANUAL_OVERRIDES[movie.id];
  const overrideByName = MANUAL_OVERRIDES[titleNorm] || MANUAL_OVERRIDES[origNorm];
  const override = overrideById || overrideByName;
  if (override) {
    const d = new Date(override.date);
    if (!isNaN(d)) return { date: d, source: 'override-manuel' };
  }

  // 2. MaxBlizz (films US uniquement — leur source couvre essentiellement le marché US)
  if (!isFrench) {
    const mb = findMaxblizzMatch(maxblizzMap, details.title, details.original_title);
    if (mb) {
      return { date: mb.date, source: 'maxblizz' };
    }
  }

  // 3. Date numérique officielle TMDB
  const officialDigital = getOfficialDigitalDate(details.release_dates);
  if (officialDigital) {
    return { date: officialDigital.date, source: `officielle-${officialDigital.region.toLowerCase()}` };
  }

  // 4. Prédiction
  const predicted = predictVODDate(cinemaDate, isFrench);
  // Snap to Tuesday : règle empirique pour les films US uniquement.
  // Garde-fou : si le snap pousse la date au-delà de la fin du mois cible,
  // on conserve la date prédite originale plutôt que d'éjecter le film.
  if (!isFrench) {
    const snapped = snapToTuesday(predicted);
    if (windowEnd && snapped > windowEnd && predicted <= windowEnd) {
      return { date: predicted, source: 'prédite' };
    }
    return { date: snapped, source: 'prédite-mardi' };
  }
  return { date: predicted, source: 'prédite' };
}

// ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

async function updateVOD() {
  const t0 = Date.now();
  console.log('🎬  updateVOD v7 — démarrage...\n');
  const now = new Date();
  const { monthStart, monthEnd, windowEnd } = computeTargetWindow(now);

  console.log(`📅  Mois cible strict : ${formatDateFR(monthStart)} → ${formatDateFR(monthEnd)}\n`);

  // 1. Préchargement : MaxBlizz
  console.log('  📡  Préchargement MaxBlizz :');
  const maxblizzMap = await fetchMaxblizzReleases();
  console.log('');

  // 2. Cache TMDB
  const cache = loadCache();
  console.log(`  💾  Cache TMDB : ${Object.keys(cache).length} entrées chargées\n`);

  // 3. Scan endpoints discover
  const endpoints = buildScanEndpoints(monthStart, windowEnd);
  const rawMovies = [];
  for (let i = 0; i < endpoints.length; i++) {
    const { name, url } = endpoints[i];
    process.stdout.write(`  🔎  [${String(i + 1).padStart(2, '0')}/${endpoints.length}] ${name.padEnd(22)} `);
    try {
      const results = await fetchAllPages(url);
      rawMovies.push(...results);
      console.log(`→ ${results.length} films`);
    } catch (err) {
      console.log(`⚠️  ${err.message}`);
    }
    await sleep(API_DELAY_MS);
  }

  const uniqueMovies = Array.from(new Map(rawMovies.map((m) => [m.id, m])).values());
  console.log(`\n  📦  ${uniqueMovies.length} films uniques\n`);

  // 4. Analyse détaillée
  const finalResults = [];
  const dropReasons = {
    noDetails: 0, noReleaseDate: 0, excludedGenre: 0, telefilmByTitle: 0,
    spectacle: 0, tooShort: 0, ghostEntry: 0, noGenresAtAll: 0,
    noFRTheatrical: 0, lowQuality: 0, beforeWindow: 0, afterWindow: 0, delayTooShort: 0,
  };
  let cacheHits = 0;
  const sourceStats = { 'override-manuel': 0, 'maxblizz': 0, 'officielle-fr': 0, 'officielle-us': 0, 'prédite': 0, 'prédite-mardi': 0 };

  console.log('  🔬  Analyse détaillée :');
  for (const movie of uniqueMovies) {
    const cachedBefore = cache[String(movie.id)] && isCacheEntryFresh(cache[String(movie.id)]);
    if (cachedBefore) cacheHits++;
    if (!cachedBefore) await sleep(API_DELAY_MS);

    let details;
    try {
      details = await fetchMovieDetails(movie.id, cache);
    } catch {
      dropReasons.noDetails++; continue;
    }

    if (!details.release_date)        { dropReasons.noReleaseDate++; continue; }
    if (hasExcludedGenre(details))    { dropReasons.excludedGenre++; continue; }
    if (isTelefilmByTitle(details))   { dropReasons.telefilmByTitle++; continue; }
    if (isSpectacle(details))         { dropReasons.spectacle++; continue; }
    if (isTooShort(details))          { dropReasons.tooShort++; continue; }
    if (isGhostEntry(details))        { dropReasons.ghostEntry++; continue; }

    if (!details.genres || details.genres.length === 0) {
      dropReasons.noGenresAtAll++; continue;
    }

    const isFrench   = isFrenchProduction(details);
    const minDelay   = isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN;
    const hasFRCine  = hasTheatricalReleaseFR(details.release_dates);

    if (!hasFRCine) { dropReasons.noFRTheatrical++; continue; }

    // Filtre Qualité Composite (v6)
    const voteCount   = details.vote_count ?? 0;
    const popularity  = movie.popularity ?? details.popularity ?? 0;
    const isSafeVolume   = voteCount >= 5;
    const isNicheButReal = popularity >= 1.5;

    if (!isSafeVolume && !isNicheButReal) {
      dropReasons.lowQuality++;
      continue;
    }

    const cinemaDateFR = getTheatricalDateFR(details.release_dates);
    const cinemaDate   = cinemaDateFR ?? new Date(details.release_date);
    if (isNaN(cinemaDate)) { dropReasons.noReleaseDate++; continue; }

    // ── Hiérarchie des sources : override > maxblizz > officielle TMDB > prédite ──
    const { date: vodDate, source } = resolveVODDate({
      details, movie, cinemaDate, isFrench, maxblizzMap, windowEnd,
    });

    // Sécurité délai : on rejette une date VOD trop proche de la sortie ciné
    // UNIQUEMENT pour les dates prédites (où l'on calcule soi-même).
    // Les sources fiables (override, maxblizz, officielles TMDB) sont acceptées telles quelles :
    // si un studio annonce une fenêtre PVOD plus courte, c'est la réalité, pas un bug.
    const isPredicted = source === 'prédite' || source === 'prédite-mardi';
    if (isPredicted) {
      const actualDelayDays = (vodDate - cinemaDate) / 86400000;
      if (actualDelayDays < minDelay) { dropReasons.delayTooShort++; continue; }
    }

    if (vodDate < monthStart) { dropReasons.beforeWindow++; continue; }
    if (vodDate > windowEnd)  { dropReasons.afterWindow++;  continue; }

    sourceStats[source] = (sourceStats[source] || 0) + 1;

    finalResults.push({
      title          : details.title || movie.title,
      plex_release   : formatDateFR(vodDate),
      tmdb_id        : movie.id,
      poster_path    : details.poster_path || movie.poster_path,
      original_title : details.original_title,
      cinema_date    : formatDateFR(cinemaDate),
      vote_average   : Math.round((details.vote_average ?? 0) * 10) / 10,
      vote_count     : voteCount,
      genres         : details.genres.map((g) => g.name),
      is_french      : isFrench,
      source,
      _sortDate      : vodDate.getTime(),
      _popularity    : popularity,
    });

    const flag = isFrench ? '🇫🇷' : '🌍';
    const srcShort = {
      'override-manuel': '★',
      'maxblizz'       : '◆',
      'officielle-fr'  : '✓',
      'officielle-us'  : '✓',
      'prédite'        : '~',
      'prédite-mardi'  : '~',
    }[source] || '?';
    console.log(`     ${flag} ${srcShort} ${(details.title || '').padEnd(45).slice(0, 45)} → ${formatDateFR(vodDate)}`);
  }

  // Tri par date VOD puis popularité
  finalResults.sort((a, b) => {
    if (a._sortDate !== b._sortDate) return a._sortDate - b._sortDate;
    return b._popularity - a._popularity;
  });

  // Dédoublonnage par titre
  const deduped = Array.from(new Map(finalResults.map((m) => [m.title.toLowerCase().trim(), m])).values());
  const output = deduped.map(({ _sortDate, _popularity, ...rest }) => rest);

  // Écriture du fichier
  fs.mkdirSync(path.dirname(DATA_PATH), { recursive: true });
  const tmpPath = DATA_PATH + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(output, null, 2), 'utf8');
  fs.renameSync(tmpPath, DATA_PATH);

  // Nettoyage cache TMDB > 7j
  const cutoff = Date.now() - 7 * 24 * 3600000;
  for (const k of Object.keys(cache)) {
    if (!cache[k]?._cachedAt || cache[k]._cachedAt < cutoff) delete cache[k];
  }
  saveCache(cache);

  // ─── Stats finales ────────────────────────────────────────────────────────
  const frCount  = output.filter((m) => m.is_french).length;
  const elapsed  = ((Date.now() - t0) / 1000).toFixed(1);

  console.log(`\n  📊  Sources :`);
  for (const [src, n] of Object.entries(sourceStats)) {
    if (n > 0) console.log(`       ${src.padEnd(20)} : ${n}`);
  }
  console.log(`\n  ⏱   Durée : ${elapsed}s — Cache hits : ${cacheHits}/${uniqueMovies.length}`);
  console.log(`  ✅  Terminé : ${output.length} films générés (${frCount} 🇫🇷). Faible qualité écartés : ${dropReasons.lowQuality}`);
}

updateVOD().catch((err) => {
  console.error('❌ Erreur fatale :', err);
  process.exit(1);
});
