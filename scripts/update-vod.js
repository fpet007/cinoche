/**
 * updateVOD.js — v7 (light)
 * =========================
 * Génère la liste des FILMS DE CINÉMA en VOD pour le mois en cours STRICT.
 * Plex FR — uniquement de vrais longs-métrages sortis en salle.
 *
 * v7 (light) :
 * ✅ Logique v6 strictement inchangée pour l'extraction TMDB.
 * ✅ Couche d'enrichissement MaxBlizz APRÈS la liste TMDB :
 *    - Scrape https://maxblizz.com/dvd-and-vod-release-dates/
 *    - Pour chaque film MaxBlizz dont la date VOD US tombe dans le mois en cours,
 *      on l'ajoute à la liste s'il n'y est pas déjà.
 *    - Hypothèse métier : ces sorties US correspondent en pratique à des releases
 *      VFQ (et pas VFF) — on les considère comme "quasi-sûres" pour les blockbusters.
 * ✅ Override manuel : Project Hail Mary forcé au 12 mai 2026 (au lieu du 28 avril).
 *
 * Hérité de v6 :
 * ✅ Mois cible strict (pas d'anticipation).
 * ✅ Filtre composite anti-fantôme/anti-théâtre.
 * ✅ Mots-clés captations théâtre (Molières, etc.).
 *
 * Usage  : node updateVOD.js
 * Cron   : 0 2 * * * (tous les soirs à 2h)
 */

'use strict';
const fs   = require('fs');
const path = require('path');

// ─── CONFIG ────────────────────────────────────────────────────────────────────

const TMDB_API_KEY = process.env.TMDB_API_KEY || '3fd2be6f0c70a2a598f084ddfb75487c';
const DATA_PATH      = path.join(__dirname, '../data/plex-upcoming.json');
const CACHE_PATH     = path.join(__dirname, '../data/.tmdb-cache.json');
const MAXBLIZZ_CACHE = path.join(__dirname, '../data/.maxblizz-cache.json');
const DELAYS = {
  FRENCH  : 120,   // Chronologie des médias FR — VOD à l'acte
  AMERICAN:  45,   // PVOD/TVOD international standard
};

// Cache TMDB : durée de validité d'une entrée détails (en heures)
const CACHE_TTL_HOURS = 24;
// Cache MaxBlizz : durée de validité (heures). Court → on capte vite les nouvelles annonces.
const MAXBLIZZ_TTL_HOURS = 12;
// Délai entre appels TMDB (limite officielle : ~50 req/s, on reste très conservateur)
const API_DELAY_MS = 130;
// Pages max par endpoint discover (TMDB renvoie max 500 pages mais 6 suffisent)
const MAX_PAGES_PER_ENDPOINT = 6;
// Durée minimum pour ne pas être un court-métrage (minutes)
const MIN_RUNTIME = 40;

/**
 * Overrides manuels — date VOD forcée pour certains films.
 * Clé = titre normalisé (sans accents, articles, ponctuation, majuscules).
 * Valeur = { date: 'YYYY-MM-DD', reason: '...' }
 *
 * Utilisé par la couche MaxBlizz : si le slug MaxBlizz matche cette clé,
 * la date d'override remplace celle annoncée par MaxBlizz.
 */
const MANUAL_OVERRIDES = {
  'project hail mary': { date: '2026-05-12', reason: 'film à fort potentiel, repoussé' },
};

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
  // --- Nouveautés V6 : Anti-Théâtre ---
  'théâtre', 'theatre', 'pièce de', 'comédie française', 'captation',
  'molière', 'charbon dans les veines'
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

// ─── HELPERS DE BASE ───────────────────────────────────────────────────────────

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchWithRetry(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url);
      if (res.status === 429) {
        await sleep(2000 * attempt);
        continue;
      }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
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

// ─── CACHE TMDB ────────────────────────────────────────────────────────────────

function loadCache() {
  try {
    const raw = fs.readFileSync(CACHE_PATH, 'utf8');
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

function saveCache(cache) {
  try {
    fs.mkdirSync(path.dirname(CACHE_PATH), { recursive: true });
    const tmp = CACHE_PATH + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(cache), 'utf8');
    fs.renameSync(tmp, CACHE_PATH);
  } catch (err) {
    console.warn(`  ⚠️  Cache non sauvegardé : ${err.message}`);
  }
}

function isCacheEntryFresh(entry) {
  if (!entry?._cachedAt) return false;
  const ageHours = (Date.now() - entry._cachedAt) / 3600000;
  return ageHours < CACHE_TTL_HOURS;
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

// ─── MODULE MAXBLIZZ (couche d'enrichissement) ────────────────────────────────
//
// Indépendant de la logique TMDB. Scrape https://maxblizz.com/dvd-and-vod-release-dates/
// pour récupérer les dates VOD US officielles annoncées, puis ces films sont
// recherchés sur TMDB pour récupérer poster/genres/etc. avant ajout à la liste finale.
//
// Hypothèse métier : les sorties VOD US des blockbusters listés par MaxBlizz
// correspondent en pratique à des releases VFQ (rarement VFF) — on les considère
// comme "quasi-sûres" pour ce mois-ci sur Plex FR.

/** Helper fetch HTML avec retry (séparé de fetchWithRetry qui parse en JSON). */
async function fetchTextWithRetry(url, retries = 3) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 updateVOD-bot' },
      });
      if (res.status === 429) { await sleep(2000 * attempt); continue; }
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.text();
    } catch (err) {
      if (attempt === retries) throw err;
      await sleep(attempt * 600);
    }
  }
}

/** Normalisation : minuscules, sans accents, sans articles, sans ponctuation. */
function normalizeTitle(s) {
  if (!s) return '';
  return s
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/['']/g, '')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/^the |^le |^la |^les |^l /, '');
}

function loadMaxblizzCache() {
  try { return JSON.parse(fs.readFileSync(MAXBLIZZ_CACHE, 'utf8')); }
  catch { return {}; }
}

function saveMaxblizzCache(data) {
  try {
    fs.mkdirSync(path.dirname(MAXBLIZZ_CACHE), { recursive: true });
    const tmp = MAXBLIZZ_CACHE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(data), 'utf8');
    fs.renameSync(tmp, MAXBLIZZ_CACHE);
  } catch (err) {
    console.warn(`  ⚠️  Cache MaxBlizz non sauvegardé : ${err.message}`);
  }
}

/**
 * Scrape MaxBlizz et retourne la liste des films avec leur date VOD US officielle.
 *
 * Stratégie :
 *  1. Page liste → extraction des URLs d'articles + slugs (le titre vient du slug)
 *  2. Pour chaque article, fetch HTML et extraction de la VRAIE date depuis le corps
 *     (les attributs alt des images sont parfois incohérents avec le contenu).
 *  3. Cache par article (TTL 7j) — les dates VOD changent rarement une fois annoncées.
 *
 * Retourne un tableau : [{ slug, title, date: Date, url }]
 */
async function fetchMaxblizzReleases() {
  const PARSER_VERSION = 3; // À incrémenter si on change l'extraction → invalide le cache
  const cache = loadMaxblizzCache();
  const cacheValid = cache._parserVersion === PARSER_VERSION;
  const articleCache = cacheValid ? (cache.articles || {}) : {};
  const listFresh = cacheValid && cache._listCachedAt
    && (Date.now() - cache._listCachedAt) / 3600000 < MAXBLIZZ_TTL_HOURS;

  if (listFresh && Array.isArray(cache.releases)) {
    console.log(`  💾 MaxBlizz : cache valide (${cache.releases.length} entrées)`);
    return cache.releases.map((r) => ({ ...r, date: new Date(r.date) }));
  }

  if (!cacheValid && cache._parserVersion !== undefined) {
    console.log(`  🔄 MaxBlizz : invalidation cache (parser v${cache._parserVersion} → v${PARSER_VERSION})`);
  }

  console.log(`  🌐 MaxBlizz : scraping en cours...`);
  const releases = [];

  try {
    // 1. Liste : extraire les URLs d'articles
    const listHtml = await fetchTextWithRetry('https://maxblizz.com/dvd-and-vod-release-dates/');
    // Regex tolérante : MaxBlizz utilise plusieurs formats de slug pour ses articles VOD :
    //   - "<titre>-vod-release-date-revealed"           (cas standard)
    //   - "<titre>-vod-and-dvd-release-date-revealed"   (sortie combinée VOD + DVD)
    //   - "<titre>-dvd-and-vod-release-date-revealed"   (ordre inversé, déjà vu)
    // On capture tous ces formats. Le "(?:...)" reste non-capturant pour ne pas
    // décaler les groupes de capture.
    const linkRegex = /href="(https:\/\/maxblizz\.com\/([a-z0-9-]+?)-(?:vod|dvd)(?:-and-(?:vod|dvd))?-release-date-revealed\/?)"/gi;
    const articles = new Map(); // url → slug
    let m;
    while ((m = linkRegex.exec(listHtml)) !== null) {
      if (!articles.has(m[1])) articles.set(m[1], m[2]);
    }
    console.log(`     ${articles.size} articles VOD trouvés sur la liste`);

    // 2. Pour chaque article : cache hit ou fetch + extraction date
    let fromCache = 0, fetched = 0, skipped = 0;
    const newArticleCache = {};

    const monthPattern = '(January|February|March|April|May|June|July|August|September|October|November|December)';
    const datePart = `${monthPattern}\\s+(\\d{1,2}),?\\s+(\\d{4})`;

    for (const [url, slug] of articles) {
      let articleData;
      const cached = articleCache[url];
      const cachedFresh = cached && (Date.now() - cached._cachedAt) / 3600000 < 24 * 7;

      if (cachedFresh) {
        articleData = cached;
        fromCache++;
      } else {
        try {
          await sleep(250); // soft rate-limit pour ne pas matraquer le site
          const html = await fetchTextWithRetry(url, 2);

          // On cherche la date VOD canonique : c'est celle en gras (<strong> ou **)
          // dans le corps de l'article, généralement dans une phrase type :
          //   "...starting <strong>May 19, 2026</strong>."
          // Les alt d'images sont parfois faux (constaté Mario Galaxy : alt="May 5"
          // alors que l'article dit "May 19"). On les ignore donc.
          //
          // On exclut l'en-tête (~2000 chars) qui contient la date de publication.
          const strongDateRegex = new RegExp(
            `(?:<strong>|\\*\\*)\\s*${datePart}\\s*(?:</strong>|\\*\\*)`,
            'gi'
          );
          const contextDateRegex = new RegExp(
            `(?:starting|available\\s+(?:on|to)|rent\\s+(?:on|it)|buy\\s+(?:on|it)|arrives?\\s+on|arriving\\s+on|release\\s+date\\s+(?:is|will\\s+be))\\s+${datePart}`,
            'gi'
          );

          const body = html.length > 3000 ? html.slice(2000) : html;
          const candidates = [];
          let dm;
          while ((dm = strongDateRegex.exec(body)) !== null) {
            candidates.push({ src: 'strong', date: new Date(`${dm[1]} ${dm[2]}, ${dm[3]} 12:00:00 UTC`) });
          }
          while ((dm = contextDateRegex.exec(body)) !== null) {
            candidates.push({ src: 'context', date: new Date(`${dm[1]} ${dm[2]}, ${dm[3]} 12:00:00 UTC`) });
          }

          // Filtre plausibilité : entre il y a 2 mois et dans 2 ans
          const valid = candidates.filter((c) =>
            !isNaN(c.date) &&
            c.date.getTime() >= Date.now() - 60 * 86400000 &&
            c.date.getTime() <= Date.now() + 730 * 86400000
          );

          // Priorité : strong (gras) > context. Plus précoce d'abord.
          let chosenDate = null;
          const strongs = valid.filter((c) => c.src === 'strong');
          const contexts = valid.filter((c) => c.src === 'context');
          if (strongs.length > 0) chosenDate = strongs.sort((a, b) => a.date - b.date)[0].date;
          else if (contexts.length > 0) chosenDate = contexts.sort((a, b) => a.date - b.date)[0].date;

          articleData = {
            _cachedAt: Date.now(),
            date: chosenDate ? chosenDate.toISOString() : null,
            slug,
          };
          fetched++;
        } catch (err) {
          articleData = { _cachedAt: Date.now(), date: null, slug, error: err.message };
        }
      }

      newArticleCache[url] = articleData;

      if (!articleData.date) { skipped++; continue; }
      const date = new Date(articleData.date);
      if (isNaN(date)) { skipped++; continue; }

      const title = slug.replace(/-/g, ' ');
      releases.push({ slug, title, date, url });
    }

    console.log(`     ✓ ${releases.length} films extraits (${fromCache} cache, ${fetched} fetchés, ${skipped} sans date)`);

    // Cache global
    saveMaxblizzCache({
      _parserVersion: PARSER_VERSION,
      _listCachedAt: Date.now(),
      articles: newArticleCache,
      releases: releases.map((r) => ({ ...r, date: r.date.toISOString() })),
    });
  } catch (err) {
    console.warn(`  ⚠️  MaxBlizz scraping a échoué : ${err.message} — on continue sans.`);
  }

  return releases;
}

/**
 * Recherche un film sur TMDB par titre (en anglais de préférence).
 * Retourne le meilleur match : year proche de l'année courante, popularité élevée.
 */
async function tmdbSearchByTitle(title, cache) {
  // Nettoie le titre : retire les préfixes "réalisateur possessif" type "lee cronins"
  // (le slug MaxBlizz est souvent "lee-cronins-the-mummy")
  const tokens = title.split(/\s+/).filter(Boolean);
  const candidates = [title];
  if (tokens.length >= 3 && tokens[0].length >= 3) {
    candidates.push(tokens.slice(1).join(' '));
  }
  if (tokens.length >= 4 && tokens[1].length >= 3) {
    candidates.push(tokens.slice(2).join(' '));
  }

  for (const query of candidates) {
    const url = `https://api.themoviedb.org/3/search/movie?api_key=${TMDB_API_KEY}&language=fr-FR&include_adult=false&query=${encodeURIComponent(query)}`;
    let data;
    try { data = await fetchWithRetry(url); }
    catch { continue; }

    const results = data?.results || [];
    if (!results.length) continue;

    // Tri : préfère films récents (2024+) avec vote_count >= 5 pour éviter les fantômes,
    // puis popularité.
    const ranked = results
      .filter((r) => {
        const year = r.release_date ? parseInt(r.release_date.slice(0, 4), 10) : 0;
        return year >= 2024 && (r.vote_count ?? 0) >= 1;
      })
      .sort((a, b) => (b.popularity ?? 0) - (a.popularity ?? 0));

    if (ranked.length > 0) return ranked[0];
  }
  return null;
}

/**
 * Enrichit la liste finale avec les films MaxBlizz du mois cible qui n'y sont pas déjà.
 */
async function enrichWithMaxblizz({ finalResults, monthStart, monthEnd, cache }) {
  console.log('\n  📡  Enrichissement MaxBlizz :');
  const mbReleases = await fetchMaxblizzReleases();
  if (mbReleases.length === 0) return { added: 0, overridden: 0 };

  // Index des films déjà présents (par TMDB id et par titre normalisé)
  const existingIds = new Set(finalResults.map((m) => m.tmdb_id));
  const existingTitles = new Set(finalResults.map((m) => normalizeTitle(m.title)));
  const existingOriginals = new Set(finalResults.map((m) => normalizeTitle(m.original_title || '')));

  let added = 0, overridden = 0, skipped = 0;
  for (const mb of mbReleases) {
    // Applique l'override manuel si présent
    const slugNorm = normalizeTitle(mb.title);
    let mbDate = mb.date;
    let isOverride = false;

    // L'override matche soit le slug brut, soit en retirant 1 ou 2 mots de préfixe
    const tokens = slugNorm.split(' ');
    const overrideKeys = [slugNorm];
    if (tokens.length >= 3) overrideKeys.push(tokens.slice(1).join(' '));
    if (tokens.length >= 4) overrideKeys.push(tokens.slice(2).join(' '));
    for (const k of overrideKeys) {
      if (MANUAL_OVERRIDES[k]) {
        mbDate = new Date(MANUAL_OVERRIDES[k].date);
        isOverride = true;
        console.log(`     ★ Override "${mb.title}" → ${formatDateFR(mbDate)} (${MANUAL_OVERRIDES[k].reason})`);
        break;
      }
    }

    // Filtre : la date doit tomber dans le mois cible
    if (mbDate < monthStart || mbDate > monthEnd) { skipped++; continue; }

    // Le film est-il déjà dans finalResults ? On compare via TMDB search puis ID.
    const tmdbHit = await tmdbSearchByTitle(mb.title, cache);
    await sleep(API_DELAY_MS);
    if (!tmdbHit) {
      console.log(`     ✗ TMDB sans match pour "${mb.title}"`);
      skipped++;
      continue;
    }

    // Déjà présent : on peut éventuellement réécrire la date si MaxBlizz est plus précis
    if (existingIds.has(tmdbHit.id)) {
      const existing = finalResults.find((m) => m.tmdb_id === tmdbHit.id);
      const oldDate = formatDateFR(new Date(existing._sortDate));
      if (existing.plex_release !== formatDateFR(mbDate)) {
        existing.plex_release = formatDateFR(mbDate);
        existing._sortDate    = mbDate.getTime();
        existing.source       = isOverride ? 'override-manuel' : 'maxblizz';
        console.log(`     ↻ "${tmdbHit.title}" : ${oldDate} → ${formatDateFR(mbDate)} (maxblizz)`);
        overridden++;
      }
      continue;
    }

    // Garde-fou : si le titre normalisé est déjà présent (autre TMDB id), on ne dédoublonne pas
    const tmdbTitleNorm = normalizeTitle(tmdbHit.title);
    const tmdbOrigNorm  = normalizeTitle(tmdbHit.original_title || '');
    if (existingTitles.has(tmdbTitleNorm) || existingOriginals.has(tmdbOrigNorm)) {
      skipped++;
      continue;
    }

    // Récupère les détails complets pour avoir genres + cinema_date
    let details;
    try {
      details = await fetchMovieDetails(tmdbHit.id, cache);
      await sleep(API_DELAY_MS);
    } catch {
      skipped++; continue;
    }

    const cinemaDate = details.release_date ? new Date(details.release_date) : null;

    finalResults.push({
      title          : details.title || tmdbHit.title,
      plex_release   : formatDateFR(mbDate),
      tmdb_id        : tmdbHit.id,
      poster_path    : details.poster_path || tmdbHit.poster_path,
      original_title : details.original_title || tmdbHit.original_title,
      cinema_date    : cinemaDate ? formatDateFR(cinemaDate) : null,
      vote_average   : Math.round((details.vote_average ?? 0) * 10) / 10,
      vote_count     : details.vote_count ?? 0,
      genres         : (details.genres || []).map((g) => g.name),
      is_french      : false, // Les films MaxBlizz sont des sorties US — VFQ assumée
      source         : isOverride ? 'override-manuel' : 'maxblizz',
      _sortDate      : mbDate.getTime(),
      _popularity    : details.popularity ?? tmdbHit.popularity ?? 0,
    });

    console.log(`     ✓ Ajouté : ${details.title || tmdbHit.title} → ${formatDateFR(mbDate)}`);
    added++;
    existingIds.add(tmdbHit.id);
  }

  console.log(`     📊 ${added} ajouts, ${overridden} dates corrigées, ${skipped} ignorés (hors mois ou déjà listés)`);
  return { added, overridden };
}

// ─── PIPELINE PRINCIPAL ───────────────────────────────────────────────────────

async function updateVOD() {
  const t0 = Date.now();
  console.log('🎬  updateVOD v7 — démarrage...\n');
  const now = new Date();
  const { monthStart, monthEnd, windowEnd } = computeTargetWindow(now);
  
  console.log(`📅  Mois cible strict : ${formatDateFR(monthStart)} → ${formatDateFR(monthEnd)}\n`);

  const cache = loadCache();
  console.log(`💾  Cache : ${Object.keys(cache).length} entrées chargées\n`);

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

  const finalResults = [];
  const dropReasons = {
    noDetails: 0, noReleaseDate: 0, excludedGenre: 0, telefilmByTitle: 0, 
    spectacle: 0, tooShort: 0, ghostEntry: 0, noGenresAtAll: 0,
    noFRTheatrical: 0, lowQuality: 0, beforeWindow: 0, afterWindow: 0, delayTooShort: 0,
  };
  let cacheHits = 0;

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
    if (hasExcludedGenre(details))    { dropReasons.excludedGenre++;   continue; }
    if (isTelefilmByTitle(details))   { dropReasons.telefilmByTitle++; continue; }
    if (isSpectacle(details))         { dropReasons.spectacle++; continue; }
    if (isTooShort(details))          { dropReasons.tooShort++; continue; }
    if (isGhostEntry(details))        { dropReasons.ghostEntry++;      continue; }

    if (!details.genres || details.genres.length === 0) {
      dropReasons.noGenresAtAll++; continue;
    }

    const isFrench   = isFrenchProduction(details);
    const minDelay   = isFrench ? DELAYS.FRENCH : DELAYS.AMERICAN;
    const hasFRCine  = hasTheatricalReleaseFR(details.release_dates);

    if (!hasFRCine) { dropReasons.noFRTheatrical++; continue; }

    // ── V6 : Filtre Qualité Composite ───────────────────────────────────────
    const voteCount = details.vote_count ?? 0;
    const popularity = movie.popularity ?? details.popularity ?? 0;
    
    const isSafeVolume = voteCount >= 5;
    const isNicheButReal = popularity >= 1.5; // Sauve les films d'auteur obscurs mais élimine le vrai "bruit"

    if (!isSafeVolume && !isNicheButReal) {
      dropReasons.lowQuality++;
      continue;
    }

    const cinemaDateFR = getTheatricalDateFR(details.release_dates);
    const cinemaDate   = cinemaDateFR ?? new Date(details.release_date);
    if (isNaN(cinemaDate)) { dropReasons.noReleaseDate++; continue; }

    const officialDigital  = getOfficialDigitalDate(details.release_dates);
    const predictedDate    = predictVODDate(cinemaDate, isFrench);

    let vodDate, source;
    if (officialDigital) {
      vodDate = officialDigital.date;
      source  = `officielle-${officialDigital.region.toLowerCase()}`;
    } else {
      vodDate = predictedDate;
      source  = 'prédite';
    }

    const actualDelayDays = (vodDate - cinemaDate) / 86400000;
    if (actualDelayDays < minDelay) { dropReasons.delayTooShort++; continue; }

    if (vodDate < monthStart) { dropReasons.beforeWindow++; continue; }
    if (vodDate > windowEnd)  { dropReasons.afterWindow++;  continue; }

    finalResults.push({
      title        : details.title || movie.title,
      plex_release : formatDateFR(vodDate),
      tmdb_id      : movie.id,
      poster_path  : details.poster_path || movie.poster_path,
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
    const srcShort = source.startsWith('officielle') ? '✓' : '~';
    console.log(`     ${flag} ${srcShort} ${(details.title || '').padEnd(45).slice(0, 45)} → ${formatDateFR(vodDate)}`);
  }

  // ─── Enrichissement MaxBlizz (v7) ──────────────────────────────────────────
  // Couche additive : on récupère les sorties VOD US annoncées sur MaxBlizz pour
  // le mois en cours, et on les ajoute à la liste si elles n'y sont pas déjà.
  await enrichWithMaxblizz({ finalResults, monthStart, monthEnd, cache });

  finalResults.sort((a, b) => {
    if (a._sortDate !== b._sortDate) return a._sortDate - b._sortDate;
    return b._popularity - a._popularity;
  });
  
  const deduped = Array.from(new Map(finalResults.map((m) => [m.title.toLowerCase().trim(), m])).values());
  const output = deduped.map(({ _sortDate, _popularity, ...rest }) => rest);

  fs.mkdirSync(path.dirname(DATA_PATH), { recursive: true });
  const tmpPath = DATA_PATH + '.tmp';
  fs.writeFileSync(tmpPath, JSON.stringify(output, null, 2), 'utf8');
  fs.renameSync(tmpPath, DATA_PATH);

  const cutoff = Date.now() - 7 * 24 * 3600000;
  for (const k of Object.keys(cache)) {
    if (!cache[k]?._cachedAt || cache[k]._cachedAt < cutoff) delete cache[k];
  }
  saveCache(cache);

  const frCount = output.filter((m) => m.is_french).length;
  console.log(`\n  ✅ Terminé : ${output.length} films générés (${frCount} 🇫🇷). Films de faible qualité écartés : ${dropReasons.lowQuality}`);
}

updateVOD().catch((err) => {
  console.error('❌ Erreur fatale :', err);
  process.exit(1);
});
