require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const fetch = require('node-fetch');
const fs = require('fs');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegPath = require('ffmpeg-static');
const { Redis } = require('@upstash/redis');
const OAuth = require('oauth-1.0a');
const crypto = require('crypto');

ffmpeg.setFfmpegPath(ffmpegPath);

const app = express();
if (!fs.existsSync('uploads')) fs.mkdirSync('uploads');

const upload = multer({ dest: 'uploads/' });
app.use(cors());
app.use(express.json());

// ─── Protezione accesso ─────────────────────────────────────────────────────
const APP_PASSWORD = process.env.APP_PASSWORD || '';

// Endpoint per verificare la password
app.post('/auth', (req, res) => {
  const { password } = req.body;
  if (!APP_PASSWORD) return res.json({ ok: true }); // nessuna password configurata
  if (password === APP_PASSWORD) return res.json({ ok: true });
  res.status(401).json({ ok: false, error: 'Password errata' });
});

// Middleware: protegge tutte le route tranne /, /auth
function authMiddleware(req, res, next) {
  if (!APP_PASSWORD) return next(); // nessuna password = accesso libero
  const token = req.headers['x-app-token'];
  if (token === APP_PASSWORD) return next();
  res.status(401).json({ error: 'Non autorizzato' });
}

// Applica protezione alle route API
app.use('/recognize', authMiddleware);
app.use('/lyrics', authMiddleware);
app.use('/translate', authMiddleware);
app.use('/counter', authMiddleware);

const TOTAL_FREE_PER_KEY = 500;

// ─── Multi-key RapidAPI ─────────────────────────────────────────────────────
// Chiavi da env (fallback)
const ENV_KEYS = (process.env.RAPIDcachedApiKeys || process.env.RAPIDAPI_KEY || '')
  .split(',')
  .map(k => k.trim())
  .filter(Boolean);

// ─── Upstash Redis per contatore + chiavi persistenti ───────────────────────
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_URL,
  token: process.env.UPSTASH_REDIS_TOKEN,
});

// Cache locale
let localCache = { keys: {}, currentIndex: 0 };
let cachedApiKeys = [...ENV_KEYS]; // cache delle chiavi API (solo stringhe per uso interno)
let cachedKeyMeta = []; // metadata delle chiavi Redis: [{ key, email, addedAt }]

function keyPrefix(key) { return key.substring(0, 8) + '...'; }

// Carica chiavi da Redis + env (dedup per valore completo)
async function loadApiKeys() {
  let redisEntries = [];
  try {
    const stored = await redis.get('lyricsync:apikeys');
    if (Array.isArray(stored)) {
      // Supporta sia vecchio formato (array di stringhe) che nuovo (array di oggetti)
      redisEntries = stored.map(entry => {
        if (typeof entry === 'string') return { key: entry, email: '', addedAt: '' };
        return entry;
      });
    }
  } catch (err) { console.warn('⚠️ Redis keys read error:', err.message); }
  cachedKeyMeta = redisEntries;
  const redisKeys = redisEntries.map(e => e.key);
  const all = [...ENV_KEYS, ...redisKeys];
  const unique = [...new Set(all)].filter(Boolean);
  cachedApiKeys = unique;
  return unique;
}

async function saveApiKeyEntries(entries) {
  // Salva su Redis solo le entry NON presenti nelle env
  const redisOnly = entries.filter(e => !ENV_KEYS.includes(e.key));
  try {
    await redis.set('lyricsync:apikeys', redisOnly);
  } catch (err) { console.warn('⚠️ Redis keys write error:', err.message); }
  cachedKeyMeta = redisOnly;
  cachedApiKeys = [...ENV_KEYS, ...redisOnly.map(e => e.key)];
  cachedApiKeys = [...new Set(cachedApiKeys)].filter(Boolean);
}

function getKeyMeta(key) {
  return cachedKeyMeta.find(e => e.key === key) || null;
}

async function loadCounter() {
  try {
    const data = await redis.get('lyricsync:counter');
    if (data && typeof data === 'object') {
      localCache = data;
      return data;
    }
  } catch (err) { console.warn('⚠️ Redis read error:', err.message); }
  return localCache;
}

async function saveCounter(data) {
  localCache = data;
  try {
    await redis.set('lyricsync:counter', data);
  } catch (err) { console.warn('⚠️ Redis write error:', err.message); }
}

function getKeyState(counter, key) {
  const prefix = keyPrefix(key);
  if (!counter.keys) counter.keys = {};
  if (!counter.keys[prefix]) {
    counter.keys[prefix] = { used: 0, exhausted: false, lastReset: new Date().toISOString().split('T')[0] };
  }
  const today = new Date().toISOString().split('T')[0];
  if (counter.keys[prefix].lastReset !== today) {
    counter.keys[prefix].exhausted = false;
    counter.keys[prefix].lastReset = today;
  }
  return counter.keys[prefix];
}

async function getActiveKey() {
  const apiKeys = await loadApiKeys();
  if (apiKeys.length === 0) return null;
  const counter = await loadCounter();
  for (let i = 0; i < apiKeys.length; i++) {
    const idx = ((counter.currentIndex || 0) + i) % apiKeys.length;
    const key = apiKeys[idx];
    const state = getKeyState(counter, key);
    if (!state.exhausted) {
      if (idx !== counter.currentIndex) {
        counter.currentIndex = idx;
        await saveCounter(counter);
      }
      return { key, index: idx, total: apiKeys.length };
    }
  }
  return null;
}

async function markKeyExhausted(key) {
  const apiKeys = cachedApiKeys;
  const counter = await loadCounter();
  const state = getKeyState(counter, key);
  state.exhausted = true;
  const currentIdx = apiKeys.indexOf(key);
  if (currentIdx >= 0) {
    counter.currentIndex = (currentIdx + 1) % apiKeys.length;
  }
  await saveCounter(counter);
  console.log(`🔑 Chiave ${keyPrefix(key)} esaurita, passo alla prossima`);
}

async function recordKeyUsage(key) {
  const counter = await loadCounter();
  const state = getKeyState(counter, key);
  state.used += 1;
  await saveCounter(counter);
  return state.used;
}

function convertToWav(inputPath) {
  return new Promise((resolve, reject) => {
    const outputPath = inputPath + '_converted.wav';
    ffmpeg(inputPath)
      .audioChannels(1).audioFrequency(44100).audioCodec('pcm_s16le').format('wav')
      .on('end', () => resolve(outputPath))
      .on('error', (err) => { console.error('❌ ffmpeg:', err.message); reject(err); })
      .save(outputPath);
  });
}

// Cerca immagine artista su iTunes
async function fetchArtistImage(artist, title) {
  try {
    const url = `https://itunes.apple.com/search?term=${encodeURIComponent(artist + ' ' + title)}&media=music&limit=1`;
    const res = await fetch(url);
    const data = await res.json();
    if (data.results?.length > 0) {
      // Usa artwork 600x600 come sfondo artista
      return data.results[0].artworkUrl100?.replace('100x100', '600x600') || null;
    }
  } catch {}
  return null;
}

app.post('/recognize', upload.single('audio'), async (req, res) => {
  let convertedPath = null;
  try {
    if (!req.file) return res.status(400).json({ error: 'Nessun file audio' });
    console.log(`🎙️ Audio ricevuto: ${(req.file.size / 1024).toFixed(0)}KB, tipo: ${req.file.mimetype || 'unknown'}`);

    // Trova chiave disponibile
    let activeKey = await getActiveKey();
    if (!activeKey) {
      console.log('❌ Tutte le chiavi API esaurite!');
      return res.status(429).json({ error: 'Tutte le chiavi API esaurite', found: false });
    }

    convertedPath = await convertToWav(req.file.path);
    const audioData = fs.readFileSync(convertedPath);
    const pcmData = audioData.slice(44);
    const base64Audio = pcmData.toString('base64');

    // Prova con la chiave attiva, se fallisce prova la successiva
    let data = null;
    let usedKey = null;
    for (let attempt = 0; attempt < cachedApiKeys.length; attempt++) {
      activeKey = await getActiveKey();
      if (!activeKey) break;

      const callNum = await recordKeyUsage(activeKey.key);
      console.log(`📊 Chiave ${keyPrefix(activeKey.key)} [${activeKey.index + 1}/${cachedApiKeys.length}] — chiamata #${callNum}`);

      const response = await fetch('https://shazam.p.rapidapi.com/songs/v2/detect', {
        method: 'POST',
        headers: {
          'content-type': 'text/plain',
          'X-RapidAPI-Key': activeKey.key,
          'X-RapidAPI-Host': 'shazam.p.rapidapi.com'
        },
        body: base64Audio
      });

      // Log status HTTP
      console.log(`📡 Shazam risposta HTTP: ${response.status}`);

      // 429 = rate limit, 402 = quota esaurita, 403 = non autorizzato → prova prossima chiave
      if (response.status === 429 || response.status === 402 || response.status === 403) {
        const errBody = await response.text();
        console.log(`⚠️ Chiave ${keyPrefix(activeKey.key)} ha risposto ${response.status}: ${errBody.substring(0, 200)}`);
        await markKeyExhausted(activeKey.key);
        continue;
      }

      if (!response.ok) {
        const errBody = await response.text();
        console.log(`❌ Shazam errore ${response.status}: ${errBody.substring(0, 300)}`);
        break;
      }

      data = await response.json();
      usedKey = activeKey;
      // Log della risposta Shazam per debug
      console.log(`🔍 Shazam response: matches=${data.matches?.length || 0}, track=${data.track ? data.track.title : 'NO'}, keys=${Object.keys(data).join(',')}`);
      break;
    }

    if (!data || !data?.track) {
      console.log(`❌ Non trovata. Risposta Shazam: ${JSON.stringify(data || {}).substring(0, 300)}`);
      return res.json({ found: false });
    }

    const track = data.track;
    const cover = track.images?.coverarthq || track.images?.coverart || '';

    // Cerca immagine artista (sfondo)
    const shazamBg = track.images?.background || null;
    const itunesBg = !shazamBg ? await fetchArtistImage(track.subtitle || '', track.title || '') : null;
    const artistImage = shazamBg || itunesBg || cover;
    console.log(`🖼️ Immagini: shazam_bg=${shazamBg ? 'SI' : 'NO'}, itunes=${itunesBg ? 'SI' : 'NO'}, cover=${cover ? 'SI' : 'NO'}`);
    console.log(`🖼️ URL finale artistImage: ${artistImage}`);

    const song = {
      found: true,
      timeskip: data.matches?.[0]?.offset || 0,
      title: track.title || '',
      artist: track.subtitle || '',
      album: track.sections?.[0]?.metadata?.find(m => m.title === 'Album')?.text || '',
      year: track.sections?.[0]?.metadata?.find(m => m.title === 'Released')?.text || '',
      cover,
      artistImage,
      shazamKey: track.key || ''
    };
    console.log(`🎵 Trovata: ${song.title} - ${song.artist} (offset: ${song.timeskip}s)`);
    res.json(song);

  } catch (err) {
    console.error('❌ Errore recognize:', err.message);
    if (!res.headersSent) res.status(500).json({ error: 'Errore interno' });
  } finally {
    if (req.file?.path) { try { fs.unlinkSync(req.file.path); } catch {} }
    if (convertedPath) { try { fs.unlinkSync(convertedPath); } catch {} }
  }
});

// ─── Cache testi su Redis ───────────────────────────────────────────────────
const LYRICS_TTL_FOUND = 60 * 60 * 24 * 180;  // 180 giorni per i testi trovati
const LYRICS_TTL_MISS = 60 * 60 * 24 * 7;     // 7 giorni per i "non trovato"

// Normalizza artista/titolo per costruire una chiave stabile
function normKeyPart(s) {
  return (s || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')   // accenti
    .replace(/\(.*?\)|\[.*?\]/g, ' ')                    // (Remastered), [Live]...
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .substring(0, 80);
}

function lyricsKey(artist, title) {
  return `lyricsync:lyrics:${normKeyPart(artist)}:${normKeyPart(title)}`;
}

// ─── Normalizzazione titoli e artisti per il matching ───────────────────────
// Suffissi da rimuovere dai titoli prima di confrontarli o cercarli su lrclib
const TITLE_SUFFIX_RE = new RegExp(
  '\\s*(?:' +
  '[\\(\\[][^\\)\\]]*(?:live|remaster(?:ed)?|remix|mono|stereo|edit|version|mix|take|demo|' +
  'alternate|acoustic|single|album|radio|extended|bonus|reprise|instrumental|' +
  'session|outtake|rehearsal|unplugged|deluxe|anniversary|expanded)[^\\)\\]]*[\\)\\]]' +
  '|' +
  '[-–—]\\s*(?:\\d{4}\\s*)?(?:[a-z0-9\']+\\s+){0,2}(?:live|remaster(?:ed)?|remix|mono|stereo|edit|version|mix|take|demo|' +
  'alternate|acoustic|single|album|radio|extended|bonus|instrumental|' +
  'session|outtake|unplugged|deluxe|anniversary|expanded)\\b.*$' +
  ')', 'gi'
);

// Rimuove featuring, suffissi live/remaster e punteggiatura: "Glorified G (Live)" -> "glorifiedg"
function normTitle(s) {
  return (s || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/\s*(?:feat\.?|ft\.?|featuring|with)\s+[^\(\)\[\]-]*/gi, ' ')
    .replace(TITLE_SUFFIX_RE, ' ')
    .replace(/[^a-z0-9]+/g, '');
}

// Versione "pulita" del titolo ma leggibile, da usare nelle query a lrclib
function cleanTitle(s) {
  return (s || '')
    .replace(/\s*(?:feat\.?|ft\.?|featuring)\s+[^\(\)\[\]]*/gi, ' ')
    .replace(TITLE_SUFFIX_RE, ' ')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

// Normalizza il nome artista: "The Rah Band" -> "rahband"
function normArtist(s) {
  return (s || '')
    .toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/^the\s+/, '')
    .replace(/\s*\(\d+\)\s*$/, '')
    .replace(/\s*(?:&|and|feat\.?|ft\.?|featuring|with)\s+.*$/i, '')
    .replace(/[^a-z0-9]+/g, '');
}

// True se due nomi artista sono la stessa entità (match esatto sul nome normalizzato)
function sameArtist(a, b) {
  const x = normArtist(a), y = normArtist(b);
  if (!x || !y) return false;
  return x === y;
}

async function getCachedLyrics(artist, title) {
  try {
    const data = await redis.get(lyricsKey(artist, title));
    if (!data) return null;
    if (typeof data === 'string') { try { return JSON.parse(data); } catch { return null; } }
    if (typeof data === 'object') return data;
  } catch (err) { console.warn('⚠️ Redis lyrics read error:', err.message); }
  return null;
}

async function setCachedLyrics(artist, title, payload) {
  try {
    const ttl = payload && payload.found ? LYRICS_TTL_FOUND : LYRICS_TTL_MISS;
    await redis.set(lyricsKey(artist, title), payload, { ex: ttl });
  } catch (err) { console.warn('⚠️ Redis lyrics write error:', err.message); }
}

// Scarica i testi da lrclib (get esatto, poi ricerca come fallback)
async function fetchLyricsFromProvider(title, artist, album) {
  // Varianti di titolo da provare: originale, poi ripulito da (Live)/(Remastered)/- 2016 Remaster
  const bare = cleanTitle(title);
  const titleVariants = [title];
  if (bare && normTitle(bare) !== normTitle('') && bare.toLowerCase() !== title.toLowerCase()) {
    titleVariants.push(bare);
  }

  // 1) get esatto — con album, poi senza (l'album del vinile spesso non coincide con lrclib)
  for (const t of titleVariants) {
    const attempts = album ? [album, null] : [null];
    for (const alb of attempts) {
      let url = `https://lrclib.net/api/get?track_name=${encodeURIComponent(t)}&artist_name=${encodeURIComponent(artist)}`;
      if (alb) url += `&album_name=${encodeURIComponent(alb)}`;
      try {
        const response = await fetch(url);
        if (response.ok) {
          const data = await response.json();
          if (data.syncedLyrics || data.plainLyrics) {
            if (t !== title) console.log(`📝 lrclib: trovato con titolo ripulito "${t}" (era "${title}")`);
            return {
              found: true,
              syncedLyrics: data.syncedLyrics || null,
              plainLyrics: data.plainLyrics || null
            };
          }
        }
      } catch (e) { console.warn(`⚠️ lrclib get "${t}": ${e.message}`); }
    }
  }

  // 2) ricerca libera, verificando che l'artista del risultato sia davvero quello giusto
  for (const t of titleVariants) {
    try {
      const searchUrl = `https://lrclib.net/api/search?q=${encodeURIComponent(t + ' ' + artist)}`;
      const searchRes = await fetch(searchUrl);
      if (!searchRes.ok) continue;
      const results = await searchRes.json();
      if (!Array.isArray(results) || results.length === 0) continue;

      const wantTitle = normTitle(t);
      const plausible = results.filter(r =>
        sameArtist(r.artistName, artist) &&
        (normTitle(r.trackName) === wantTitle ||
         normTitle(r.trackName).includes(wantTitle) ||
         wantTitle.includes(normTitle(r.trackName)))
      );
      const pool = plausible.length ? plausible : [];
      if (!pool.length) continue;

      const best = pool.find(r => r.syncedLyrics) || pool[0];
      if (t !== title) console.log(`📝 lrclib: trovato via search con "${t}" (era "${title}")`);
      return {
        found: true,
        syncedLyrics: best.syncedLyrics || null,
        plainLyrics: best.plainLyrics || null
      };
    } catch (e) { console.warn(`⚠️ lrclib search "${t}": ${e.message}`); }
  }

  return { found: false, syncedLyrics: null, plainLyrics: null };
}

// Testi con cache Redis: prima la cache, poi lrclib
async function getLyrics(title, artist, album) {
  const cached = await getCachedLyrics(artist, title);
  if (cached) {
    console.log(`💾 Cache Redis hit: "${title}" - "${artist}" (found: ${cached.found})`);
    return { ...cached, cached: true };
  }
  const result = await fetchLyricsFromProvider(title, artist, album);
  await setCachedLyrics(artist, title, result);
  return { ...result, cached: false };
}

app.get('/lyrics', async (req, res) => {
  try {
    const { title, artist, album } = req.query;
    if (!title || !artist) return res.status(400).json({ error: 'Parametri mancanti' });
    console.log(`🔍 Cerco testi: "${title}" - "${artist}"`);

    const result = await getLyrics(title, artist, album);
    if (result.found) console.log(`✅ Testo trovato, synced: ${!!result.syncedLyrics}, da cache: ${result.cached}`);
    else console.log(`❌ Testo non trovato per "${title}"`);
    res.json(result);

  } catch (err) {
    console.error('❌ Errore lyrics:', err.message);
    res.status(500).json({ error: 'Errore interno' });
  }
});

// ─── Prefetch testi di un intero album ──────────────────────────────────────
const PREFETCH_CONCURRENCY = 4;              // richieste lrclib in parallelo
const PREFETCH_JOB_TTL = 60 * 60 * 6;        // 6 ore di vita per lo stato del job
const prefetchJobs = new Map();              // cache in memoria: jobId -> stato

function prefetchJobId(artist, album) {
  return `${normKeyPart(artist)}:${normKeyPart(album)}`;
}
function prefetchJobKey(jobId) {
  return `lyricsync:prefetch:${jobId}`;
}

async function saveJobState(state) {
  prefetchJobs.set(state.jobId, state);
  try {
    await redis.set(prefetchJobKey(state.jobId), state, { ex: PREFETCH_JOB_TTL });
  } catch (err) { console.warn('⚠️ Redis prefetch write error:', err.message); }
}

async function loadJobState(jobId) {
  const mem = prefetchJobs.get(jobId);
  if (mem) return mem;
  try {
    const data = await redis.get(prefetchJobKey(jobId));
    if (!data) return null;
    const parsed = typeof data === 'string' ? JSON.parse(data) : data;
    if (parsed && parsed.jobId) {
      // Un job rimasto "running" da più di 5 minuti è morto (restart del server)
      if (parsed.running && Date.now() - (parsed.startedAt || 0) > 300000) parsed.running = false;
      prefetchJobs.set(jobId, parsed);
      return parsed;
    }
  } catch { }
  return null;
}

// ─── Tracklist di una release, con cache Redis (le tracklist non cambiano mai) ─
const TRACKLIST_TTL = 60 * 60 * 24 * 365;   // 1 anno

async function fetchReleaseTracklist(releaseId, cfg) {
  const id = String(releaseId);
  const key = `lyricsync:tracklist:${id}`;
  try {
    const cached = await redis.get(key);
    if (cached) {
      const parsed = typeof cached === 'string' ? JSON.parse(cached) : cached;
      if (Array.isArray(parsed)) return parsed;
    }
  } catch { }

  try {
    const authHeader = `Discogs key=${cfg.consumerKey}, secret=${cfg.consumerSecret}`;
    const res = await fetch(`https://api.discogs.com/releases/${id}`, {
      headers: { 'Authorization': authHeader, 'User-Agent': 'LyricSync/1.0' }
    });
    if (!res.ok) {
      console.warn(`⚠️ Tracklist release ${id}: HTTP ${res.status}`);
      return [];
    }
    const data = await res.json();
    const tracks = (data.tracklist || [])
      .filter(t => t.type_ === 'track' && t.title)
      .map(t => ({
        position: t.position || '',
        title: String(t.title).trim(),
        duration: t.duration || ''
      }));
    try { await redis.set(key, tracks, { ex: TRACKLIST_TTL }); } catch { }
    return tracks;
  } catch (e) {
    console.warn(`⚠️ Tracklist release ${id}: ${e.message}`);
    return [];
  }
}

// True se il brano riconosciuto compare nella tracklist (confronto normalizzato)
function tracklistHasTrack(tracks, title) {
  const want = normTitle(title);
  if (!want || !Array.isArray(tracks) || tracks.length === 0) return false;
  return tracks.some(t => {
    const have = normTitle(t.title);
    if (!have) return false;
    if (have === want) return true;
    // Tolleranza per titoli lunghi (es. "Working John, Working Joe" vs varianti)
    if (want.length >= 8 && have.length >= 8) {
      return have.includes(want) || want.includes(have);
    }
    return false;
  });
}

// Trova la tracklist di un album: prima per releaseId, poi nella collezione, poi sul database Discogs
async function resolveAlbumTracklist({ artist, album, releaseId }) {
  const cfg = await loadDiscogsConfig();
  if (!cfg) return { releaseId: null, tracks: [], reason: 'Discogs non configurato' };

  const authHeader = `Discogs key=${cfg.consumerKey}, secret=${cfg.consumerSecret}`;
  const headers = { 'Authorization': authHeader, 'User-Agent': 'LyricSync/1.0' };

  let id = releaseId ? String(releaseId) : null;

  // 1) Cerca l'album nella collezione già in cache (nessuna chiamata API)
  if (!id && album) {
    try {
      const collection = await loadFullCollection(cfg);
      const albumNorm = normTitle(album);
      const match = collection.find(r => {
        const titleNorm = normTitle(r.title);
        const titleOk = titleNorm === albumNorm ||
          (albumNorm.length >= 4 && (titleNorm.includes(albumNorm) || albumNorm.includes(titleNorm)));
        if (!titleOk) return false;
        if (!artist) return true;
        return r.artists.some(a => sameArtist(a, artist));
      });
      if (match) {
        id = String(match.id);
        console.log(`📀 Prefetch: album "${album}" trovato in collezione (release ${id})`);
      }
    } catch (e) { console.warn('⚠️ Prefetch collection lookup error:', e.message); }
  }

  // 2) Fallback: cerca sul database Discogs
  if (!id && album) {
    try {
      const q = `${artist || ''} ${album}`.trim();
      const searchUrl = `https://api.discogs.com/database/search?q=${encodeURIComponent(q)}&type=release&per_page=1`;
      const sRes = await fetch(searchUrl, { headers });
      if (sRes.ok) {
        const sData = await sRes.json();
        if (sData.results?.length > 0) {
          id = String(sData.results[0].id);
          console.log(`📀 Prefetch: album "${album}" trovato sul database Discogs (release ${id})`);
        }
      }
    } catch (e) { console.warn('⚠️ Prefetch discogs search error:', e.message); }
  }

  if (!id) return { releaseId: null, tracks: [], reason: 'Album non trovato su Discogs' };

  // 3) Scarica la tracklist della release
  try {
    const rRes = await fetch(`https://api.discogs.com/releases/${id}`, { headers });
    if (!rRes.ok) return { releaseId: id, tracks: [], reason: `Discogs release ${rRes.status}` };
    const rData = await rRes.json();
    const tracks = (rData.tracklist || [])
      .filter(t => t.type_ === 'track' && t.title)
      .map(t => ({
        position: t.position || '',
        title: t.title.trim(),
        duration: t.duration || ''
      }));
    return { releaseId: id, tracks, reason: null };
  } catch (e) {
    return { releaseId: id, tracks: [], reason: e.message };
  }
}

// Esegue il prefetch in background con un pool di worker
async function runPrefetch(state, artist) {
  const tracks = state.tracks;
  let cursor = 0;
  let lastSave = 0;

  const flush = async (force = false) => {
    const now = Date.now();
    if (force || now - lastSave > 900) { lastSave = now; await saveJobState(state); }
  };

  async function worker() {
    while (cursor < tracks.length) {
      const i = cursor++;
      const t = tracks[i];
      try {
        const cached = await getCachedLyrics(artist, t.title);
        if (cached) {
          t.status = cached.found ? 'ready' : 'missing';
          t.synced = !!cached.syncedLyrics;
        } else {
          const result = await fetchLyricsFromProvider(t.title, artist, state.album);
          await setCachedLyrics(artist, t.title, result);
          t.status = result.found ? 'ready' : 'missing';
          t.synced = !!result.syncedLyrics;
        }
      } catch (e) {
        t.status = 'error';
        console.warn(`⚠️ Prefetch "${t.title}": ${e.message}`);
      }
      state.done++;
      await flush();
    }
  }

  const workers = Array.from({ length: Math.min(PREFETCH_CONCURRENCY, tracks.length) }, () => worker());
  await Promise.all(workers);

  state.running = false;
  state.finishedAt = Date.now();
  const ready = tracks.filter(t => t.status === 'ready').length;
  console.log(`📀 Prefetch completato "${state.album}": ${ready}/${tracks.length} testi disponibili in ${((state.finishedAt - state.startedAt) / 1000).toFixed(1)}s`);
  await flush(true);
}

// Avvia (o riprende) il prefetch dei testi di un album — risponde subito, lavora in background
app.get('/lyrics/prefetch', async (req, res) => {
  try {
    const { artist, album, releaseId, force } = req.query;
    if (!artist || !album) return res.status(400).json({ error: 'Parametri mancanti (artist, album)' });

    const jobId = prefetchJobId(artist, album);
    const existing = await loadJobState(jobId);

    // Job già in corso o già completato di recente: restituisci lo stato senza rifare nulla
    if (existing && force !== '1') {
      if (existing.running) return res.json({ ...existing, resumed: true });
      const pending = existing.tracks.some(t => t.status === 'pending' || t.status === 'error');
      if (!pending) return res.json({ ...existing, resumed: true });
    }

    const { releaseId: resolvedId, tracks, reason } = await resolveAlbumTracklist({ artist, album, releaseId });
    if (tracks.length === 0) {
      const empty = {
        jobId, artist, album, releaseId: resolvedId,
        total: 0, done: 0, running: false, tracks: [],
        startedAt: Date.now(), finishedAt: Date.now(), reason
      };
      console.log(`📀 Prefetch "${album}": nessuna traccia (${reason || 'tracklist vuota'})`);
      return res.json(empty);
    }

    const state = {
      jobId,
      artist,
      album,
      releaseId: resolvedId,
      total: tracks.length,
      done: 0,
      running: true,
      startedAt: Date.now(),
      finishedAt: null,
      reason: null,
      tracks: tracks.map(t => ({ ...t, status: 'pending', synced: false }))
    };

    await saveJobState(state);
    console.log(`📀 Prefetch avviato "${album}" (${artist}): ${tracks.length} tracce`);

    // Non aspettare: il lavoro continua dopo la risposta
    runPrefetch(state, artist).catch(err => {
      console.error('❌ Prefetch error:', err.message);
      state.running = false;
      state.reason = err.message;
      saveJobState(state);
    });

    res.json(state);

  } catch (err) {
    console.error('❌ Errore prefetch:', err.message);
    res.status(500).json({ error: 'Errore interno' });
  }
});

// Stato del prefetch — il frontend fa polling qui per aggiornare i pallini
app.get('/lyrics/prefetch/status', async (req, res) => {
  try {
    const { artist, album } = req.query;
    if (!artist || !album) return res.status(400).json({ error: 'Parametri mancanti (artist, album)' });
    const state = await loadJobState(prefetchJobId(artist, album));
    if (!state) return res.json({ found: false, running: false, tracks: [], total: 0, done: 0 });
    res.json({ found: true, ...state });
  } catch (err) {
    console.error('❌ Errore prefetch status:', err.message);
    res.status(500).json({ error: 'Errore interno' });
  }
});

// Traduzione testi via MyMemory (gratuito, 5000 parole/giorno)
app.post('/translate', async (req, res) => {
  try {
    const { text, lines: inputLines, targetLang = 'it' } = req.body;
    if (!text && !inputLines) return res.status(400).json({ error: 'Testo mancante' });

    const sourceLang = 'en';

    // Se riceviamo un array di righe, traduci a blocchi preservando l'allineamento
    if (inputLines && Array.isArray(inputLines)) {
      const translated = [];
      // Traduci in blocchi da 8 righe per evitare il limite di 5000 char
      const CHUNK_SIZE = 8;
      for (let i = 0; i < inputLines.length; i += CHUNK_SIZE) {
        const chunk = inputLines.slice(i, i + CHUNK_SIZE);
        // Numera ogni riga come ancora per riallineamento
        const numbered = chunk.map((line, idx) => `[${i + idx}] ${line || '...'}`).join('\n');
        const url = `https://api.mymemory.translated.net/get?q=${encodeURIComponent(numbered.substring(0, 5000))}&langpair=${sourceLang}|${targetLang}&de=rogermi@gmail.com`;
        const response = await fetch(url);
        const data = await response.json();

        if (data.responseStatus === 200 && data.responseData?.translatedText) {
          const translatedText = data.responseData.translatedText;
          // Riestrai le righe usando i numeri come ancore
          for (let j = 0; j < chunk.length; j++) {
            const lineNum = i + j;
            const nextNum = lineNum + 1;
            // Cerca [N] ... fino a [N+1] o fine testo
            const regex = new RegExp(`\\[${lineNum}\\]\\s*(.+?)(?=\\s*\\[${nextNum}\\]|$)`, 's');
            const match = translatedText.match(regex);
            translated.push(match ? match[1].trim() : chunk[j]); // fallback: originale
          }
        } else {
          // Fallback: usa le righe originali
          chunk.forEach(line => translated.push(line));
        }
      }
      return res.json({ translated: translated.join('\n'), sourceLang, targetLang });
    }

    // Fallback legacy: testo unico (per plainLyrics)
    const url = `https://api.mymemory.translated.net/get?q=${encodeURIComponent(text.substring(0, 5000))}&langpair=${sourceLang}|${targetLang}&de=rogermi@gmail.com`;
    const response = await fetch(url);
    const data = await response.json();

    if (data.responseStatus === 200 && data.responseData?.translatedText) {
      res.json({ translated: data.responseData.translatedText, sourceLang, targetLang });
    } else {
      res.json({ error: 'Traduzione non disponibile' });
    }
  } catch (err) {
    console.error('❌ Errore translate:', err.message);
    res.status(500).json({ error: 'Errore traduzione' });
  }
});

app.get('/counter', async (req, res) => {
  const counter = await loadCounter();
  let totalUsed = 0;
  let totalRemaining = 0;
  const keyDetails = [];
  for (const key of cachedApiKeys) {
    const state = getKeyState(counter, key);
    totalUsed += state.used;
    const keyRemaining = state.exhausted ? 0 : Math.max(0, TOTAL_FREE_PER_KEY - state.used);
    totalRemaining += keyRemaining;
    keyDetails.push({
      key: keyPrefix(key),
      used: state.used,
      exhausted: state.exhausted,
      remaining: keyRemaining
    });
  }
  const totalCapacity = cachedApiKeys.length * TOTAL_FREE_PER_KEY;
  res.json({
    used: totalUsed,
    remaining: totalRemaining,
    total: totalCapacity,
    keysCount: cachedApiKeys.length,
    activeKeyIndex: counter.currentIndex || 0,
    keys: keyDetails
  });
});

// Reset contatore (utile a inizio mese quando RapidAPI resetta la quota)
app.post('/counter/reset', async (req, res) => {
  try {
    const counter = await loadCounter();
    for (const key of cachedApiKeys) {
      const prefix = keyPrefix(key);
      if (counter.keys && counter.keys[prefix]) {
        counter.keys[prefix].used = 0;
        counter.keys[prefix].exhausted = false;
        counter.keys[prefix].lastReset = new Date().toISOString().split('T')[0];
      }
    }
    counter.currentIndex = 0;
    await saveCounter(counter);
    console.log('🔄 Contatori resettati manualmente');
    res.json({ success: true, message: 'Tutti i contatori resettati' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Discogs integration ────────────────────────────────────────────────────
// Auth su tutte le route /discogs TRANNE /discogs/oauth/callback (redirect da Discogs, senza header)
app.use('/discogs', (req, res, next) => {
  if (req.path === '/oauth/callback') return next();
  authMiddleware(req, res, next);
});

// Carica/salva config Discogs su Redis
async function loadDiscogsConfig() {
  try {
    const cfg = await redis.get('lyricsync:discogs');
    if (cfg && typeof cfg === 'object') return cfg;
  } catch {}
  return null;
}
async function saveDiscogsConfig(cfg) {
  try { await redis.set('lyricsync:discogs', cfg); } catch (err) { console.warn('⚠️ Redis discogs write error:', err.message); }
}

// ─── OAuth 1.0a per Discogs (scrittura campi collezione) ────────────────────
async function loadDiscogsOAuth() {
  try {
    const tokens = await redis.get('lyricsync:discogs:oauth');
    if (tokens && typeof tokens === 'object' && tokens.accessToken) return tokens;
  } catch {}
  return null;
}

async function saveDiscogsOAuth(tokens) {
  try { await redis.set('lyricsync:discogs:oauth', tokens); } catch (err) { console.warn('⚠️ Redis discogs oauth write error:', err.message); }
}

function createOAuthClient(cfg) {
  return OAuth({
    consumer: { key: cfg.consumerKey, secret: cfg.consumerSecret },
    signature_method: 'HMAC-SHA1',
    hash_function(baseString, key) {
      return crypto.createHmac('sha1', key).update(baseString).digest('base64');
    }
  });
}

// Temporary storage for request tokens (Redis, sopravvive ai restart del server)
async function savePendingOAuthToken(oauthToken, secret) {
  try {
    await redis.set(`lyricsync:discogs:pending:${oauthToken}`, JSON.stringify({ secret, ts: Date.now() }));
    // Scade dopo 10 minuti
    await redis.expire(`lyricsync:discogs:pending:${oauthToken}`, 600);
  } catch (err) { console.warn('⚠️ Redis pending oauth write error:', err.message); }
}

async function getPendingOAuthToken(oauthToken) {
  try {
    const data = await redis.get(`lyricsync:discogs:pending:${oauthToken}`);
    if (!data) return null;
    const parsed = typeof data === 'string' ? JSON.parse(data) : data;
    return parsed;
  } catch { return null; }
}

async function deletePendingOAuthToken(oauthToken) {
  try { await redis.del(`lyricsync:discogs:pending:${oauthToken}`); } catch {}
}

// Step 1: Get request token and return authorization URL
app.get('/discogs/oauth/start', authMiddleware, async (req, res) => {
  try {
    const cfg = await loadDiscogsConfig();
    if (!cfg) return res.status(400).json({ error: 'Discogs non configurato' });

    const oauth = createOAuthClient(cfg);
    const requestTokenUrl = 'https://api.discogs.com/oauth/request_token';
    // callback_url: il frontend aprirà una finestra popup che redirige qui
    const callbackUrl = req.query.callback || `${req.protocol}://${req.get('host')}/discogs/oauth/callback`;

    const requestData = {
      url: requestTokenUrl,
      method: 'GET',
      data: { oauth_callback: callbackUrl }
    };

    const authHeader = oauth.toHeader(oauth.authorize(requestData));
    authHeader['User-Agent'] = 'LyricSync/1.0';

    const response = await fetch(requestTokenUrl + '?oauth_callback=' + encodeURIComponent(callbackUrl), {
      headers: authHeader
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error(`❌ Discogs OAuth request token error: ${response.status} ${errText}`);
      return res.status(response.status).json({ error: 'Errore richiesta token OAuth', details: errText });
    }

    const body = await response.text();
    const params = new URLSearchParams(body);
    const oauthToken = params.get('oauth_token');
    const oauthTokenSecret = params.get('oauth_token_secret');

    if (!oauthToken || !oauthTokenSecret) {
      return res.status(500).json({ error: 'Token OAuth non ricevuti da Discogs' });
    }

    // Salva temporaneamente il token secret su Redis (serve per step 3, scade in 10 min)
    await savePendingOAuthToken(oauthToken, oauthTokenSecret);

    const authorizeUrl = `https://www.discogs.com/oauth/authorize?oauth_token=${oauthToken}`;
    console.log(`🔐 Discogs OAuth: request token ottenuto, redirect a ${authorizeUrl}`);
    res.json({ authorizeUrl, oauthToken });

  } catch (err) {
    console.error('❌ Discogs OAuth start error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Step 2: Callback — Discogs redirige qui dopo l'autorizzazione dell'utente
app.get('/discogs/oauth/callback', async (req, res) => {
  try {
    const { oauth_token, oauth_verifier } = req.query;
    if (!oauth_token || !oauth_verifier) {
      return res.status(400).send('<html><body><h2>Errore: parametri OAuth mancanti</h2></body></html>');
    }

    const pending = await getPendingOAuthToken(oauth_token);
    if (!pending) {
      return res.status(400).send('<html><body><h2>Errore: token OAuth scaduto o non valido. Riprova.</h2></body></html>');
    }

    const cfg = await loadDiscogsConfig();
    if (!cfg) {
      return res.status(400).send('<html><body><h2>Errore: Discogs non configurato</h2></body></html>');
    }

    const oauth = createOAuthClient(cfg);
    const accessTokenUrl = 'https://api.discogs.com/oauth/access_token';

    const requestData = {
      url: accessTokenUrl,
      method: 'POST',
      data: { oauth_verifier }
    };

    const token = { key: oauth_token, secret: pending.secret };
    const authHeader = oauth.toHeader(oauth.authorize(requestData, token));
    authHeader['User-Agent'] = 'LyricSync/1.0';
    authHeader['Content-Type'] = 'application/x-www-form-urlencoded';

    const response = await fetch(accessTokenUrl, {
      method: 'POST',
      headers: authHeader,
      body: `oauth_verifier=${encodeURIComponent(oauth_verifier)}`
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error(`❌ Discogs OAuth access token error: ${response.status} ${errText}`);
      return res.status(response.status).send(`<html><body><h2>Errore OAuth: ${response.status}</h2><p>${errText}</p></body></html>`);
    }

    const body = await response.text();
    const params = new URLSearchParams(body);
    const accessToken = params.get('oauth_token');
    const accessTokenSecret = params.get('oauth_token_secret');

    if (!accessToken || !accessTokenSecret) {
      return res.status(500).send('<html><body><h2>Errore: access token non ricevuti</h2></body></html>');
    }

    // Salva i token di accesso su Redis
    await saveDiscogsOAuth({ accessToken, accessTokenSecret, authorizedAt: new Date().toISOString() });
    await deletePendingOAuthToken(oauth_token);

    console.log(`🔐 Discogs OAuth: autorizzazione completata! Token salvati.`);

    // Pagina HTML di successo che chiude la finestra popup
    res.send(`<!DOCTYPE html>
<html><head><title>LyricSync - Discogs Autorizzato</title>
<style>body{font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;background:#1a1a2e;color:#e8c97e;}
.box{text-align:center;}.ok{font-size:3rem;margin-bottom:1rem;}</style></head>
<body><div class="box"><div class="ok">✅</div><h2>Discogs autorizzato!</h2><p>Puoi chiudere questa finestra.</p>
<script>setTimeout(()=>{window.close()},2000)</script></div></body></html>`);

  } catch (err) {
    console.error('❌ Discogs OAuth callback error:', err.message);
    res.status(500).send(`<html><body><h2>Errore: ${err.message}</h2></body></html>`);
  }
});

// Controlla stato OAuth
app.get('/discogs/oauth/status', authMiddleware, async (req, res) => {
  const tokens = await loadDiscogsOAuth();
  if (tokens) {
    // Verifica che i token funzionino testando l'identità
    let identity = null;
    try {
      const cfg = await loadDiscogsConfig();
      if (cfg) {
        const oauth = createOAuthClient(cfg);
        const idUrl = 'https://api.discogs.com/oauth/identity';
        const idReq = { url: idUrl, method: 'GET' };
        const token = { key: tokens.accessToken, secret: tokens.accessTokenSecret };
        const idHeader = oauth.toHeader(oauth.authorize(idReq, token));
        idHeader['User-Agent'] = 'LyricSync/1.0';
        const idRes = await fetch(idUrl, { headers: idHeader });
        if (idRes.ok) {
          identity = await idRes.json();
          console.log(`🔐 OAuth identity: ${identity.username} (id: ${identity.id})`);
        } else {
          console.warn(`⚠️ OAuth identity check failed: ${idRes.status}`);
        }
      }
    } catch (e) { console.warn('⚠️ OAuth identity error:', e.message); }

    res.json({ authorized: true, authorizedAt: tokens.authorizedAt || '', username: identity?.username || '' });
  } else {
    res.json({ authorized: false });
  }
});

// Revoca OAuth (rimuovi token)
app.post('/discogs/oauth/revoke', authMiddleware, async (req, res) => {
  try {
    await redis.del('lyricsync:discogs:oauth');
    console.log('🔐 Discogs OAuth: token revocati');
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Get/Set config
app.get('/discogs/config', async (req, res) => {
  const cfg = await loadDiscogsConfig();
  if (cfg) {
    res.json({ configured: true, username: cfg.username });
  } else {
    res.json({ configured: false });
  }
});

app.post('/discogs/config', async (req, res) => {
  const { username, consumerKey, consumerSecret } = req.body;
  if (!username || !consumerKey || !consumerSecret) return res.status(400).json({ error: 'Dati mancanti' });
  await saveDiscogsConfig({ username, consumerKey, consumerSecret });
  console.log(`💿 Discogs configurato: utente ${username}`);
  res.json({ ok: true });
});

// ─── Cache collezione Discogs ────────────────────────────────────────────
let collectionCache = null;
let collectionCacheTime = 0;
const COLLECTION_CACHE_TTL = 3600000; // 1 ora

async function loadFullCollection(cfg) {
  const now = Date.now();
  // Usa cache in memoria se fresca
  if (collectionCache && (now - collectionCacheTime) < COLLECTION_CACHE_TTL) {
    console.log(`💿 Collezione da cache in memoria (${collectionCache.length} release)`);
    return collectionCache;
  }
  // Prova cache Redis
  try {
    const cached = await redis.get('lyricsync:discogs:collection');
    if (cached && Array.isArray(cached)) {
      const cacheAge = await redis.get('lyricsync:discogs:collection_ts');
      if (cacheAge && (now - Number(cacheAge)) < COLLECTION_CACHE_TTL) {
        collectionCache = cached;
        collectionCacheTime = Number(cacheAge);
        console.log(`💿 Collezione da cache Redis (${cached.length} release)`);
        return cached;
      }
    }
  } catch {}

  // Carica da API Discogs (paginata)
  const authHeader = `Discogs key=${cfg.consumerKey}, secret=${cfg.consumerSecret}`;
  const userAgent = 'LyricSync/1.0';
  const allReleases = [];
  let page = 1;
  let totalPages = 1;

  console.log(`💿 Caricamento collezione Discogs per "${cfg.username}"...`);
  while (page <= totalPages && page <= 20) { // max 20 pagine (2000 release)
    const url = `https://api.discogs.com/users/${cfg.username}/collection/folders/0/releases?per_page=100&page=${page}&sort=artist&sort_order=asc`;
    const res = await fetch(url, {
      headers: { 'Authorization': authHeader, 'User-Agent': userAgent }
    });
    if (!res.ok) {
      console.log(`❌ Discogs collection page ${page} error: ${res.status}`);
      break;
    }
    const data = await res.json();
    totalPages = data.pagination?.pages || 1;

    for (const item of (data.releases || [])) {
      const info = item.basic_information || {};
      allReleases.push({
        id: item.id,
        instanceId: item.instance_id,
        rating: item.rating || 0,
        notes: item.notes || [],
        title: info.title || '',
        artists: (info.artists || []).map(a => a.name.replace(/ \(\d+\)$/, '')),
        year: info.year || 0,
        labels: (info.labels || []).map(l => ({ name: l.name, catno: l.catno })),
        formats: (info.formats || []).map(f => `${f.name} ${(f.descriptions || []).join(', ')}`),
        cover: info.cover_image || info.thumb || '',
        folderId: item.folder_id
      });
    }
    console.log(`💿 Collezione pagina ${page}/${totalPages}: ${data.releases?.length || 0} release`);
    page++;
  }

  console.log(`💿 Collezione caricata: ${allReleases.length} release totali`);

  // Salva in cache
  collectionCache = allReleases;
  collectionCacheTime = now;
  try {
    await redis.set('lyricsync:discogs:collection', allReleases);
    await redis.set('lyricsync:discogs:collection_ts', String(now));
  } catch (e) { console.warn('⚠️ Redis collection cache write error:', e.message); }

  return allReleases;
}

// Endpoint per forzare refresh della cache collezione
app.post('/discogs/refresh', async (req, res) => {
  const cfg = await loadDiscogsConfig();
  if (!cfg) return res.status(400).json({ error: 'Discogs non configurato' });
  collectionCache = null;
  collectionCacheTime = 0;
  const collection = await loadFullCollection(cfg);
  res.json({ ok: true, count: collection.length });
});

// Nomi campi personalizzati Discogs (salvati su Redis)
app.get('/discogs/fields', async (req, res) => {
  try {
    const saved = await redis.get('lyricsync:discogs:fields');
    res.json({ fields: saved || { 1: 'Media Condition', 2: 'Sleeve Condition', 3: 'Notes' } });
  } catch { res.json({ fields: {} }); }
});

app.post('/discogs/fields', async (req, res) => {
  try {
    const { fields } = req.body; // { "1": "Media Condition", "2": "Sleeve Condition", "3": "Notes", "4": "Anno" }
    if (!fields || typeof fields !== 'object') return res.status(400).json({ error: 'Dati mancanti' });
    await redis.set('lyricsync:discogs:fields', fields);
    console.log(`💿 Discogs fields salvati: ${JSON.stringify(fields)}`);
    res.json({ ok: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// Cerca album nella collezione Discogs
app.get('/discogs/search', async (req, res) => {
  try {
    const { artist, title, album } = req.query;
    if (!artist) return res.status(400).json({ error: 'Parametri mancanti' });
    const cfg = await loadDiscogsConfig();
    if (!cfg) return res.json({ found: false, reason: 'Discogs non configurato' });

    // Carica nomi campi personalizzati
    const DEFAULT_FIELDS = { 1: 'Media Condition', 2: 'Sleeve Condition', 3: 'Notes' };
    let customFields = { ...DEFAULT_FIELDS };
    try {
      const savedFields = await redis.get('lyricsync:discogs:fields');
      if (savedFields && typeof savedFields === 'object') {
        Object.assign(customFields, savedFields);
      }
    } catch {}

    // ─── STEP 1: Cerca nella collezione cachata (veloce, nessuna chiamata API) ───
    const collection = await loadFullCollection(cfg);

    // Candidati: release della collezione il cui artista corrisponde ESATTAMENTE
    // (niente includes: "Heart" non deve pescare "Tom Petty & The Heartbreakers")
    const matches = collection.filter(r => r.artists.some(a => sameArtist(a, artist)));

    if (matches.length > 0) {
      console.log(`💿 Discogs: ${matches.length} release di "${artist}" nella tua collezione`);

      const albumNorm = album ? normTitle(album) : '';
      const shazamYear = parseInt(req.query.year, 10) || 0;

      // Ordina i candidati: prima quelli col titolo album compatibile con Shazam,
      // così nella maggior parte dei casi la verifica si ferma alla prima release.
      const scored = matches.map(r => {
        const tNorm = normTitle(r.title);
        let priority = 3;
        if (albumNorm && tNorm === albumNorm) priority = 0;
        else if (albumNorm && (tNorm.includes(albumNorm) || albumNorm.includes(tNorm))) priority = 1;
        else if (!albumNorm) priority = 2;
        const yearGap = shazamYear && r.year ? Math.abs(r.year - shazamYear) : 999;
        return { r, priority, yearGap };
      }).sort((a, b) => a.priority - b.priority || a.yearGap - b.yearGap);

      // ─── Verifica che il brano riconosciuto sia DAVVERO nella tracklist ───
      // Senza questo controllo l'app si aggancia al primo disco dell'artista
      // (es. Vs. dei Pearl Jam → MTV Unplugged) e ci resta incollata.
      const MAX_CANDIDATES = 8;   // limite per non saturare le API Discogs
      let best = null;
      let bestTracklist = [];
      const checked = [];

      if (title) {
        for (const cand of scored.slice(0, MAX_CANDIDATES)) {
          const tl = await fetchReleaseTracklist(cand.r.id, cfg);
          checked.push({ cand, tl });
          if (tracklistHasTrack(tl, title)) {
            best = cand.r;
            bestTracklist = tl;
            console.log(`💿 Match confermato: "${best.title}" contiene "${title}"`);
            break;
          }
        }
        // Pareggio già risolto dall'ordinamento (priorità titolo, poi vicinanza anno)
        if (!best) {
          console.log(`💿 Nessuna release in collezione contiene "${title}" — niente album, solo dati Shazam`);
          return res.json({
            found: false,
            reason: 'track-not-in-collection',
            checked: checked.length
          });
        }
      } else {
        // Senza titolo non possiamo verificare nulla: usa il candidato meglio piazzato
        best = scored[0].r;
        bestTracklist = await fetchReleaseTracklist(best.id, cfg);
      }

      // Mappa le note con nomi campi
      const notes = (best.notes || [])
        .filter(n => n.value !== undefined && n.value !== null && String(n.value).trim() !== '')
        .map(n => ({
          fieldId: n.field_id,
          fieldName: customFields[n.field_id] || `Campo ${n.field_id}`,
          value: String(n.value).trim()
        }));

      // Tracklist già scaricata (e messa in cache) durante la verifica del match
      const tracklist = bestTracklist;
      console.log(`💿 Discogs tracklist: ${tracklist.length} tracce`);

      const result = {
        found: true,
        inCollection: true,
        releaseId: best.id,
        instanceId: best.instanceId,
        folderId: best.folderId || 1,  // folder 0 = "All" (virtuale, non scrivibile), 1 = "Uncategorized" (default reale)
        title: best.title || '',
        artist: best.artists.join(', ') || '',
        year: best.year || '',
        label: best.labels.map(l => l.name).join(', ') || '',
        catno: best.labels[0]?.catno || '',
        format: best.formats.join(' / ') || '',
        cover: best.cover || '',
        discogsUrl: `https://www.discogs.com/release/${best.id}`,
        notes,
        rating: best.rating || 0,
        tracklist
      };
      console.log(`💿 Discogs: "${result.title}" IN COLLEZIONE (${result.label}, ${result.year}, folder: ${result.folderId}, instance: ${result.instanceId}, notes: ${notes.length}, tracks: ${tracklist.length})`);
      return res.json(result);
    }

    console.log(`💿 Discogs: "${artist}" non trovato in collezione (${collection.length} release)`);

    // ─── STEP 2: Fallback — cerca su Discogs database (non in collezione) ───
    const authHeader = `Discogs key=${cfg.consumerKey}, secret=${cfg.consumerSecret}`;
    const userAgent = 'LyricSync/1.0';
    const query = album ? `${artist} ${album}` : `${artist} ${title}`;
    const searchUrl = `https://api.discogs.com/database/search?q=${encodeURIComponent(query)}&type=release&per_page=5`;
    const searchRes = await fetch(searchUrl, {
      headers: { 'Authorization': authHeader, 'User-Agent': userAgent }
    });

    if (searchRes.ok) {
      const data = await searchRes.json();
      // I risultati Discogs hanno title nel formato "Artista - Album": accetta
      // solo quelli il cui artista corrisponde davvero a quello riconosciuto.
      const plausible = (data.results || []).filter(r => {
        const parts = String(r.title || '').split(' - ');
        return parts.length < 2 ? false : sameArtist(parts[0], artist);
      });
      if (plausible.length > 0) {
        const first = plausible[0];
        console.log(`💿 Discogs: "${first.title}" trovato ma NON in collezione`);
        return res.json({
          found: true,
          inCollection: false,
          releaseId: first.id,
          title: first.title || '',
          year: first.year || '',
          label: first.label?.[0] || '',
          format: first.format?.join(', ') || '',
          cover: first.cover_image || '',
          discogsUrl: `https://www.discogs.com/release/${first.id}`
        });
      }
    }

    res.json({ found: false });

  } catch (err) {
    console.error('❌ Discogs error:', err.message);
    res.json({ found: false, error: err.message });
  }
});

// ─── Aggiorna campo personalizzato Discogs (data ascolto) — usa OAuth 1.0a ──
app.post('/discogs/update-field', authMiddleware, async (req, res) => {
  try {
    const { releaseId, instanceId, folderId, fieldId, value } = req.body;
    if (!releaseId || !instanceId || !fieldId) {
      return res.status(400).json({ error: 'Parametri mancanti (releaseId, instanceId, fieldId)' });
    }
    const cfg = await loadDiscogsConfig();
    if (!cfg) return res.status(400).json({ error: 'Discogs non configurato' });

    // Controlla se abbiamo token OAuth (necessari per la scrittura)
    const oauthTokens = await loadDiscogsOAuth();
    if (!oauthTokens) {
      console.warn(`⚠️ Discogs update field: OAuth non autorizzato, impossibile scrivere`);
      return res.json({ success: false, reason: 'oauth_required', message: 'Autorizza Discogs OAuth nelle impostazioni per abilitare la scrittura.' });
    }

    const folder = folderId || 1;  // Mai usare folder 0 ("All"), default a 1 ("Uncategorized")
    console.log(`💿 Discogs update: folderId ricevuto=${folderId}, usato=${folder}, release=${releaseId}, instance=${instanceId}, field=${fieldId}`);
    const url = `https://api.discogs.com/users/${cfg.username}/collection/folders/${folder}/releases/${releaseId}/instances/${instanceId}/fields/${fieldId}`;

    // Firma la richiesta con OAuth 1.0a
    // Per endpoint JSON: il body NON viene incluso nella firma OAuth
    const oauth = createOAuthClient(cfg);
    const requestData = { url, method: 'POST' };
    const token = { key: oauthTokens.accessToken, secret: oauthTokens.accessTokenSecret };
    const oauthHeader = oauth.toHeader(oauth.authorize(requestData, token));

    console.log(`💿 Discogs update: POST ${url}`);
    console.log(`💿 Discogs update: OAuth token=${oauthTokens.accessToken.substring(0, 8)}...`);

    const apiRes = await fetch(url, {
      method: 'POST',
      headers: {
        ...oauthHeader,
        'User-Agent': 'LyricSync/1.0',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ value: String(value) })
    });

    console.log(`💿 Discogs update response: ${apiRes.status}`);

    if (apiRes.status === 204 || apiRes.ok) {
      console.log(`💿 Discogs: campo ${fieldId} aggiornato per release ${releaseId} → "${value}"`);
      return res.json({ success: true });
    }

    const errText = await apiRes.text();
    console.error(`❌ Discogs update field: ${apiRes.status} ${errText}`);

    if (apiRes.status === 401) {
      console.warn(`⚠️ Discogs update field 401: token OAuth scaduto o revocato`);
      return res.json({ success: false, reason: 'oauth_expired', message: 'Token OAuth scaduto. Riautorizza Discogs nelle impostazioni.' });
    }
    if (apiRes.status === 403) {
      return res.json({ success: false, reason: 'oauth_required', message: 'Errore autorizzazione Discogs. Riprova l\'autorizzazione OAuth.' });
    }
    res.status(apiRes.status).json({ error: `Discogs API error: ${apiRes.status}`, details: errText });

  } catch (err) {
    console.error('❌ Discogs update field error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── Immagini release Discogs (per galleria) ────────────────────────────────
app.get('/discogs/images/:releaseId', authMiddleware, async (req, res) => {
  try {
    const { releaseId } = req.params;
    const cfg = await loadDiscogsConfig();
    if (!cfg) return res.json({ images: [] });

    const authHeader = `Discogs key=${cfg.consumerKey}, secret=${cfg.consumerSecret}`;
    const apiRes = await fetch(`https://api.discogs.com/releases/${releaseId}`, {
      headers: { 'Authorization': authHeader, 'User-Agent': 'LyricSync/1.0' }
    });
    if (!apiRes.ok) return res.json({ images: [] });

    const data = await apiRes.json();
    const images = (data.images || []).map(img => ({
      type: img.type || 'secondary',  // primary, secondary
      uri: img.uri || img.resource_url || '',
      uri150: img.uri150 || '',
      width: img.width || 0,
      height: img.height || 0
    })).filter(img => img.uri);

    console.log(`🖼️ Discogs: ${images.length} immagini per release ${releaseId}`);
    res.json({ images });
  } catch (err) {
    console.error('❌ Discogs images error:', err.message);
    res.json({ images: [] });
  }
});

// ─── Info artista da Wikipedia IT ───────────────────────────────────────────
app.get('/artist/info', authMiddleware, async (req, res) => {
  try {
    const { artist } = req.query;
    if (!artist) return res.status(400).json({ error: 'Parametro artist mancante' });
    const ua = { 'User-Agent': 'LyricSync/1.0' };

    // Strategia: usa Wikidata per trovare l'entità musicale giusta, poi prendi l'articolo Wikipedia IT
    // Step 1: cerca su Wikidata per entità di tipo musicista/band
    let pageTitle = null;
    try {
      const wdUrl = `https://www.wikidata.org/w/api.php?action=wbsearchentities&search=${encodeURIComponent(artist)}&language=it&format=json&limit=10&type=item`;
      const wdRes = await fetch(wdUrl, { headers: ua });
      const wdData = await wdRes.json();

      if (wdData.search?.length) {
        // Per ogni risultato, controlla se è un musicista/band guardando le claims P31 (instance of)
        // Cerchiamo: Q5 (human) con P106 (occupation) musicale, oppure Q215380 (band)
        // Approccio semplificato: prendi i primi 3 candidati e verifica su Wikipedia IT
        for (const entity of wdData.search.slice(0, 5)) {
          // Controlla se ha un sitelink a it.wikipedia
          const entityUrl = `https://www.wikidata.org/w/api.php?action=wbgetentities&ids=${entity.id}&props=sitelinks|claims&sitefilter=itwiki&format=json`;
          const entityRes = await fetch(entityUrl, { headers: ua });
          const entityData = await entityRes.json();
          const ent = entityData.entities?.[entity.id];
          if (!ent) continue;

          // Ha un articolo su Wikipedia IT?
          const itTitle = ent.sitelinks?.itwiki?.title;
          if (!itTitle) continue;

          // Verifica che sia legato alla musica: P31 contiene Q5/Q215380/Q2088357 o P106 contiene musicista
          const claims = ent.claims || {};
          const instanceOf = (claims.P31 || []).map(c => c.mainsnak?.datavalue?.value?.id);
          const occupations = (claims.P106 || []).map(c => c.mainsnak?.datavalue?.value?.id);
          const genres = claims.P136 || []; // ha genere musicale

          const isMusical = instanceOf.some(id => ['Q215380', 'Q2088357', 'Q5741069', 'Q56816954'].includes(id)) || // band, musical group
            occupations.some(id => ['Q177220', 'Q639669', 'Q36834', 'Q488205', 'Q753110', 'Q855091'].includes(id)) || // singer, musician, composer, singer-songwriter
            genres.length > 0;

          // Il nome deve corrispondere: senza questo controllo "The Rah Band"
          // finiva per pescare la prima entità musicale disponibile (es. Queen).
          const nameOk = sameArtist(entity.label, artist) ||
            sameArtist(itTitle.replace(/\s*\([^)]*\)\s*$/, ''), artist) ||
            (entity.aliases || []).some(al => sameArtist(al, artist)) ||
            sameArtist(entity.match?.text, artist);

          if (isMusical && nameOk) {
            pageTitle = itTitle;
            console.log(`📖 Wikidata: "${artist}" → ${entity.id} → "${itTitle}" (musicale)`);
            break;
          }
          if (isMusical && !nameOk) {
            console.log(`📖 Wikidata: scarto "${entity.label}" — non corrisponde a "${artist}"`);
          }
        }
      }
    } catch (e) { console.warn('⚠️ Wikidata search error:', e.message); }

    // Fallback: cerca direttamente su Wikipedia IT
    if (!pageTitle) {
      const searchUrl = `https://it.wikipedia.org/w/api.php?action=query&list=search&srsearch=${encodeURIComponent(artist)}&format=json&srlimit=3&utf8=1`;
      const searchRes = await fetch(searchUrl, { headers: ua });
      const searchData = await searchRes.json();
      if (searchData.query?.search?.length) {
        // Accetta solo se il titolo della pagina corrisponde all'artista:
        // altrimenti si finiva a mostrare la biografia di un'altra band.
        const hit = searchData.query.search.find(s =>
          sameArtist(s.title.replace(/\s*\([^)]*\)\s*$/, ''), artist)
        );
        if (hit) {
          pageTitle = hit.title;
          console.log(`📖 Wikipedia fallback: "${artist}" → "${pageTitle}"`);
        } else {
          console.log(`📖 Wikipedia: nessuna pagina corrispondente a "${artist}" — nessuna bio`);
        }
      }
    }

    if (!pageTitle) return res.json({ found: false });

    // Step 2: ottieni l'estratto della pagina
    const extractUrl = `https://it.wikipedia.org/w/api.php?action=query&titles=${encodeURIComponent(pageTitle)}&prop=extracts|pageimages&exintro=false&explaintext=true&exsectionformat=plain&pithumbsize=400&format=json&utf8=1`;
    const extractRes = await fetch(extractUrl, { headers: ua });
    const extractData = await extractRes.json();

    const pages = extractData.query?.pages || {};
    const page = Object.values(pages)[0];
    if (!page || page.missing !== undefined) {
      return res.json({ found: false });
    }

    // Limita il testo a ~3000 caratteri per non appesantire il frontend
    let extract = page.extract || '';
    if (extract.length > 3000) {
      extract = extract.substring(0, 3000);
      const lastPeriod = extract.lastIndexOf('.');
      if (lastPeriod > 2000) extract = extract.substring(0, lastPeriod + 1);
      extract += '\n\n[...]';
    }

    const wikiUrl = `https://it.wikipedia.org/wiki/${encodeURIComponent(pageTitle.replace(/ /g, '_'))}`;
    console.log(`📖 Wikipedia IT: "${pageTitle}" (${extract.length} chars)`);

    res.json({
      found: true,
      title: pageTitle,
      extract,
      image: page.thumbnail?.source || null,
      wikiUrl
    });

  } catch (err) {
    console.error('❌ Artist info error:', err.message);
    res.json({ found: false, error: err.message });
  }
});

// ─── Discografia artista da Wikipedia + evidenza collezione Discogs ──────────
app.get('/discogs/discography', authMiddleware, async (req, res) => {
  try {
    const { artist } = req.query;
    if (!artist) return res.status(400).json({ error: 'Parametro artist mancante' });
    const ua = { 'User-Agent': 'LyricSync/1.0' };

    // Step 1: trova la pagina Wikipedia IT dell'artista (riusa logica Wikidata)
    let pageTitle = null;
    try {
      const wdUrl = `https://www.wikidata.org/w/api.php?action=wbsearchentities&search=${encodeURIComponent(artist)}&language=it&format=json&limit=10&type=item`;
      const wdRes = await fetch(wdUrl, { headers: ua });
      const wdData = await wdRes.json();
      if (wdData.search?.length) {
        for (const entity of wdData.search.slice(0, 5)) {
          const entityUrl = `https://www.wikidata.org/w/api.php?action=wbgetentities&ids=${entity.id}&props=sitelinks|claims&sitefilter=itwiki&format=json`;
          const entityRes = await fetch(entityUrl, { headers: ua });
          const entityData = await entityRes.json();
          const ent = entityData.entities?.[entity.id];
          if (!ent) continue;
          const itTitle = ent.sitelinks?.itwiki?.title;
          if (!itTitle) continue;
          const claims = ent.claims || {};
          const instanceOf = (claims.P31 || []).map(c => c.mainsnak?.datavalue?.value?.id);
          const occupations = (claims.P106 || []).map(c => c.mainsnak?.datavalue?.value?.id);
          const genres = claims.P136 || [];
          const isMusical = instanceOf.some(id => ['Q215380', 'Q2088357', 'Q5741069', 'Q56816954'].includes(id)) ||
            occupations.some(id => ['Q177220', 'Q639669', 'Q36834', 'Q488205', 'Q753110', 'Q855091'].includes(id)) ||
            genres.length > 0;
          const nameOk = sameArtist(entity.label, artist) ||
            sameArtist(itTitle.replace(/\s*\([^)]*\)\s*$/, ''), artist) ||
            (entity.aliases || []).some(al => sameArtist(al, artist)) ||
            sameArtist(entity.match?.text, artist);
          if (isMusical && nameOk) { pageTitle = itTitle; break; }
        }
      }
    } catch {}

    if (!pageTitle) {
      // Fallback: cerca direttamente, ma solo con corrispondenza esatta del nome
      const searchUrl = `https://it.wikipedia.org/w/api.php?action=query&list=search&srsearch=${encodeURIComponent(artist)}&format=json&srlimit=3&utf8=1`;
      const searchRes = await fetch(searchUrl, { headers: ua });
      const searchData = await searchRes.json();
      const hit = (searchData.query?.search || []).find(s =>
        sameArtist(s.title.replace(/\s*\([^)]*\)\s*$/, ''), artist)
      );
      if (hit) pageTitle = hit.title;
    }

    if (!pageTitle) {
      console.log(`📖 Discografia: nessuna pagina corrispondente a "${artist}"`);
      return res.json({ found: false, artist });
    }

    // Step 2: cerca la pagina "Discografia di <artista>" su Wikipedia IT
    let discographyTitle = null;
    const discTitles = [`Discografia di ${pageTitle || artist}`, `Discografia dei ${pageTitle || artist}`, `Discografia delle ${pageTitle || artist}`];
    for (const dt of discTitles) {
      const checkUrl = `https://it.wikipedia.org/w/api.php?action=query&titles=${encodeURIComponent(dt)}&format=json&utf8=1`;
      const checkRes = await fetch(checkUrl, { headers: ua });
      const checkData = await checkRes.json();
      const p = Object.values(checkData.query?.pages || {})[0];
      if (p && p.missing === undefined) { discographyTitle = dt; break; }
    }

    // Fallback: cerca "discografia" nell'articolo dell'artista (sezione)
    let releases = [];

    if (discographyTitle) {
      // Pagina dedicata alla discografia: estrai la sezione "Album in studio"
      const wikiUrl = `https://it.wikipedia.org/w/api.php?action=parse&page=${encodeURIComponent(discographyTitle)}&prop=wikitext&format=json&utf8=1`;
      const wikiRes = await fetch(wikiUrl, { headers: ua });
      const wikiData = await wikiRes.json();
      const wikitext = wikiData.parse?.wikitext?.['*'] || '';

      // Estrai album dalla sezione "Album in studio" o "Album" del wikitext
      releases = parseDiscographyWikitext(wikitext);
      console.log(`📀 Discografia Wikipedia: "${discographyTitle}" → ${releases.length} album`);
    } else if (pageTitle) {
      // Prova a estrarre la sezione discografia dall'articolo principale
      const secUrl = `https://it.wikipedia.org/w/api.php?action=parse&page=${encodeURIComponent(pageTitle)}&prop=sections&format=json&utf8=1`;
      const secRes = await fetch(secUrl, { headers: ua });
      const secData = await secRes.json();
      const sections = secData.parse?.sections || [];
      const discoSec = sections.find(s => s.line.toLowerCase().includes('discografia'));
      if (discoSec) {
        const secTextUrl = `https://it.wikipedia.org/w/api.php?action=parse&page=${encodeURIComponent(pageTitle)}&prop=wikitext&section=${discoSec.index}&format=json&utf8=1`;
        const secTextRes = await fetch(secTextUrl, { headers: ua });
        const secTextData = await secTextRes.json();
        const wikitext = secTextData.parse?.wikitext?.['*'] || '';
        releases = parseDiscographyWikitext(wikitext);
        console.log(`📀 Discografia da sezione articolo: "${pageTitle}" → ${releases.length} album`);
      }
    }

    // Step 3: per ogni album, cerca cover su Discogs e incrocia con la collezione
    const cfg = await loadDiscogsConfig();
    let collection = [];
    if (cfg) {
      collection = await loadFullCollection(cfg);
    }

    // Normalizza titoli collezione per confronto fuzzy
    const collectionNormalized = collection.map(r => ({
      ...r,
      titleNorm: r.title.toLowerCase().replace(/[^a-z0-9]/g, '')
    }));

    const authHeader = cfg ? `Discogs key=${cfg.consumerKey}, secret=${cfg.consumerSecret}` : '';
    const userAgent = 'LyricSync/1.0';

    // Arricchisci ogni album con cover e stato collezione
    const enriched = [];
    for (const r of releases) {
      const titleNorm = r.title.toLowerCase().replace(/[^a-z0-9]/g, '');
      let thumb = '';
      let inCollection = false;

      // Matching con la collezione — fuzzy: includes bidirezionale + ratio lunghezza
      for (const cr of collectionNormalized) {
        if (cr.titleNorm === titleNorm) {
          inCollection = true;
          thumb = cr.cover || '';
          break;
        }
        // includes bidirezionale con ratio
        const shorter = cr.titleNorm.length < titleNorm.length ? cr.titleNorm : titleNorm;
        const longer = cr.titleNorm.length < titleNorm.length ? titleNorm : cr.titleNorm;
        if (shorter.length >= 4 && shorter.length / longer.length >= 0.6 && longer.includes(shorter)) {
          // Verifica che l'artista corrisponda
          const artistNorm = (pageTitle || artist).toLowerCase().replace(/[^a-z0-9]/g, '');
          const crArtistMatch = cr.artists.some(a => {
            const aN = a.toLowerCase().replace(/[^a-z0-9]/g, '');
            return aN === artistNorm || aN.includes(artistNorm) || artistNorm.includes(aN);
          });
          if (crArtistMatch) {
            inCollection = true;
            thumb = cr.cover || '';
            break;
          }
        }
      }

      // Se non abbiamo la cover dalla collezione, cerca su Discogs
      if (!thumb && cfg) {
        try {
          const searchQ = `${artist} ${r.title}`;
          const searchUrl = `https://api.discogs.com/database/search?q=${encodeURIComponent(searchQ)}&type=master&per_page=1`;
          const sRes = await fetch(searchUrl, {
            headers: { 'Authorization': authHeader, 'User-Agent': userAgent }
          });
          if (sRes.ok) {
            const sData = await sRes.json();
            if (sData.results?.length > 0) {
              thumb = sData.results[0].thumb || sData.results[0].cover_image || '';
            }
          }
        } catch {}
      }

      enriched.push({ ...r, thumb, inCollection });
    }

    const totalInCollection = enriched.filter(r => r.inCollection).length;
    console.log(`📀 Discografia finale: ${enriched.length} album, ${totalInCollection} in collezione`);

    res.json({
      artist: pageTitle || artist,
      releases: enriched,
      totalInCollection
    });

  } catch (err) {
    console.error('❌ Discography error:', err.message);
    res.json({ releases: [], error: err.message });
  }
});

// Parser wikitext discografia — estrae titoli e anni
function parseDiscographyWikitext(wikitext) {
  const releases = [];
  const lines = wikitext.split('\n');
  let inStudioSection = false;
  let sectionDepth = 0;

  for (const line of lines) {
    // Rileva sezioni: === Album in studio === o == Album ==
    const sectionMatch = line.match(/^(={2,})\s*(.+?)\s*={2,}/);
    if (sectionMatch) {
      const depth = sectionMatch[1].length;
      const name = sectionMatch[2].toLowerCase();
      if (name.includes('album in studio') || name.includes('album studio') ||
          (name === 'album' && !name.includes('live') && !name.includes('raccolt'))) {
        inStudioSection = true;
        sectionDepth = depth;
      } else if (inStudioSection && depth <= sectionDepth) {
        inStudioSection = false; // fine sezione album in studio
      }
      continue;
    }

    // Se siamo nella sezione giusta o non abbiamo trovato una sezione specifica
    // Cerca pattern: * [[anno]] - ''[[titolo]]'' oppure * anno – titolo
    if (line.startsWith('*')) {
      // Pattern: * [[1969]] - ''[[Let It Bleed]]''
      // Pattern: * ''[[Sticky Fingers]]'' ([[1971]])
      // Pattern: * 1969 – Let It Bleed
      let title = null;
      let year = null;

      // Estrai anno
      const yearMatch = line.match(/\b(19[5-9]\d|20[0-2]\d)\b/);
      if (yearMatch) year = parseInt(yearMatch[1]);

      // Estrai titolo: prova ''[[titolo]]'' o ''titolo''
      const titleMatch = line.match(/''(?:\[\[)?([^'\]\|]+)/);
      if (titleMatch) {
        title = titleMatch[1].trim();
      } else {
        // Prova [[titolo]]
        const linkMatch = line.match(/\[\[([^\]\|]+)/);
        if (linkMatch && !linkMatch[1].match(/^\d{4}$/)) {
          title = linkMatch[1].trim();
        }
      }

      // Fallback: prova pattern "anno – titolo" o "anno - titolo"
      if (!title) {
        const dashMatch = line.match(/\d{4}\s*[–\-]\s*(.+)/);
        if (dashMatch) {
          title = dashMatch[1].replace(/[\[\]'{}]/g, '').trim();
        }
      }

      if (title && title.length > 1) {
        // Pulisci il titolo da markup wiki residuo
        title = title.replace(/\[\[|\]\]/g, '').replace(/''/g, '').replace(/\{\{.*?\}\}/g, '').trim();
        if (title && !releases.find(r => r.title === title)) {
          releases.push({
            title,
            year: year || 0,
            thumb: '',
            type: inStudioSection ? 'studio' : 'album'
          });
        }
      }
    }
  }

  // Se non abbiamo trovato una sezione "album in studio", restituisci tutto
  // Se l'abbiamo trovata, filtra solo quelli marcati come studio
  if (releases.some(r => r.type === 'studio')) {
    return releases.filter(r => r.type === 'studio');
  }
  return releases;
}

// ─── Gestione chiavi API da frontend ─────────────────────────────────────────
app.use('/keys', authMiddleware);

// Lista chiavi (mascherate, con metadata)
app.get('/keys', async (req, res) => {
  const apiKeys = await loadApiKeys();
  const counter = await loadCounter();
  const keys = apiKeys.map((key, i) => {
    const state = getKeyState(counter, key);
    const meta = getKeyMeta(key);
    return {
      index: i,
      prefix: keyPrefix(key),
      source: ENV_KEYS.includes(key) ? 'env' : 'redis',
      email: meta?.email || '',
      addedAt: meta?.addedAt || '',
      used: state.used,
      exhausted: state.exhausted,
      remaining: state.exhausted ? 0 : Math.max(0, TOTAL_FREE_PER_KEY - state.used)
    };
  });
  res.json({ keys, total: apiKeys.length });
});

// Aggiungi chiave con email e data
app.post('/keys', async (req, res) => {
  const { key, email } = req.body;
  if (!key || key.trim().length < 10) return res.status(400).json({ error: 'Chiave non valida' });
  const apiKeys = await loadApiKeys();
  if (apiKeys.includes(key.trim())) return res.status(400).json({ error: 'Chiave già presente' });
  // Aggiungi come entry con metadata
  const newEntry = { key: key.trim(), email: (email || '').trim(), addedAt: new Date().toISOString().split('T')[0] };
  const entries = [...cachedKeyMeta, newEntry];
  await saveApiKeyEntries(entries);
  console.log(`🔑 Nuova chiave aggiunta: ${keyPrefix(key.trim())} (${newEntry.email || 'no email'}) — totale: ${cachedApiKeys.length}`);
  res.json({ ok: true, total: cachedApiKeys.length });
});

// Rimuovi chiave (solo quelle da Redis, non da env)
app.delete('/keys/:index', async (req, res) => {
  const idx = parseInt(req.params.index);
  const apiKeys = await loadApiKeys();
  if (idx < 0 || idx >= apiKeys.length) return res.status(400).json({ error: 'Indice non valido' });
  const key = apiKeys[idx];
  if (ENV_KEYS.includes(key)) return res.status(400).json({ error: 'Non puoi rimuovere chiavi da variabili d\'ambiente' });
  const entries = cachedKeyMeta.filter(e => e.key !== key);
  await saveApiKeyEntries(entries);
  console.log(`🔑 Chiave rimossa: ${keyPrefix(key)} (totale: ${cachedApiKeys.length})`);
  res.json({ ok: true, total: cachedApiKeys.length });
});

app.get('/', (req, res) => res.json({ status: 'LyricSync backend attivo ✅' }));

const PORT = process.env.PORT || 3001;
app.listen(PORT, async () => {
  console.log(`✅ LyricSync backend attivo su http://localhost:${PORT}`);
  const apiKeys = await loadApiKeys();
  console.log(`🔑 Chiavi API: ${apiKeys.length} (${ENV_KEYS.length} da env + ${apiKeys.length - ENV_KEYS.length} da Redis) — capacità: ${apiKeys.length * TOTAL_FREE_PER_KEY} chiamate`);
  console.log(`💾 Redis: ${process.env.UPSTASH_REDIS_URL ? 'configurato' : '⚠️ NON configurato!'}`);
  const counter = await loadCounter();
  apiKeys.forEach((key, i) => {
    const state = getKeyState(counter, key);
    console.log(`   [${i + 1}] ${keyPrefix(key)} — usate: ${state.used}, esaurita: ${state.exhausted}`);
  });
});
