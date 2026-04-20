require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const fetch = require('node-fetch');
const fs = require('fs');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegPath = require('ffmpeg-static');
const { Redis } = require('@upstash/redis');

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

app.get('/lyrics', async (req, res) => {
  try {
    const { title, artist, album } = req.query;
    if (!title || !artist) return res.status(400).json({ error: 'Parametri mancanti' });
    console.log(`🔍 Cerco testi: "${title}" - "${artist}"`);

    let url = `https://lrclib.net/api/get?track_name=${encodeURIComponent(title)}&artist_name=${encodeURIComponent(artist)}`;
    if (album) url += `&album_name=${encodeURIComponent(album)}`;
    let response = await fetch(url);
    console.log('📬 lrclib get:', response.status);

    if (!response.ok) {
      const searchUrl = `https://lrclib.net/api/search?q=${encodeURIComponent(title + ' ' + artist)}`;
      const searchRes = await fetch(searchUrl);
      if (searchRes.ok) {
        const results = await searchRes.json();
        if (results.length > 0) {
          return res.json({ found: true, syncedLyrics: results[0].syncedLyrics || null, plainLyrics: results[0].plainLyrics || null });
        }
      }
      return res.json({ found: false });
    }

    const data = await response.json();
    console.log(`✅ Testo trovato, synced: ${!!data.syncedLyrics}`);
    res.json({ found: true, syncedLyrics: data.syncedLyrics || null, plainLyrics: data.plainLyrics || null });

  } catch (err) {
    console.error('❌ Errore lyrics:', err.message);
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
app.use('/discogs', authMiddleware);

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

    const authHeader = `Discogs key=${cfg.consumerKey}, secret=${cfg.consumerSecret}`;
    const userAgent = 'LyricSync/1.0';

    // Nomi campi personalizzati: prova dall'API, altrimenti usa i default Discogs
    const DEFAULT_FIELDS = { 1: 'Media Condition', 2: 'Sleeve Condition', 3: 'Notes' };
    let customFields = { ...DEFAULT_FIELDS };
    try {
      const fieldsRes = await fetch(`https://api.discogs.com/users/${cfg.username}/collection/fields`, {
        headers: { 'Authorization': authHeader, 'User-Agent': userAgent }
      });
      if (fieldsRes.ok) {
        const fieldsData = await fieldsRes.json();
        for (const f of (fieldsData.fields || [])) {
          customFields[f.id] = f.name;
        }
        console.log(`💿 Discogs fields da API: ${JSON.stringify(customFields)}`);
      } else {
        // Carica nomi custom da Redis (se configurati dall'utente)
        try {
          const savedFields = await redis.get('lyricsync:discogs:fields');
          if (savedFields && typeof savedFields === 'object') {
            Object.assign(customFields, savedFields);
          }
        } catch {}
        console.log(`💿 Discogs fields (fallback): ${JSON.stringify(customFields)}`);
      }
    } catch (e) {
      console.warn('⚠️ Discogs fields error:', e.message);
    }

    // Strategia di ricerca Discogs:
    // 1) Cerca per artista + traccia (trova album che contengono il brano)
    // 2) Fallback: cerca per artista + album (da Shazam)
    const searches = [
      { label: 'artist+track', url: `https://api.discogs.com/database/search?artist=${encodeURIComponent(artist)}&track=${encodeURIComponent(title)}&type=release&per_page=15` },
    ];
    if (album) {
      searches.push({ label: 'artist+album', url: `https://api.discogs.com/database/search?artist=${encodeURIComponent(artist)}&title=${encodeURIComponent(album)}&type=release&per_page=10` });
    }

    let searchData = null;
    let searchLabel = '';
    for (const s of searches) {
      console.log(`💿 Discogs ricerca [${s.label}]: artist="${artist}", title/track="${title}", album="${album || ''}"`)
      const searchRes = await fetch(s.url, {
        headers: { 'Authorization': authHeader, 'User-Agent': userAgent }
      });
      if (!searchRes.ok) {
        console.log(`❌ Discogs search [${s.label}] error: ${searchRes.status}`);
        continue;
      }
      const data = await searchRes.json();
      if (data.results && data.results.length > 0) {
        searchData = data;
        searchLabel = s.label;
        console.log(`💿 Discogs [${s.label}]: ${data.results.length} risultati`);
        break;
      }
      console.log(`💿 Discogs [${s.label}]: 0 risultati, provo prossima strategia`);
    }

    if (!searchData || !searchData.results || searchData.results.length === 0) {
      return res.json({ found: false });
    }

    // Per ogni risultato, controlla se è DAVVERO nella collezione dell'utente
    for (const release of searchData.results) {
      const collUrl = `https://api.discogs.com/users/${cfg.username}/collection/releases/${release.id}`;
      try {
        const collRes = await fetch(collUrl, {
          headers: { 'Authorization': authHeader, 'User-Agent': userAgent }
        });

        if (!collRes.ok) continue;
        const collData = await collRes.json();

        // FIX: l'endpoint ritorna 200 anche se NON in collezione — controllare che ci siano release
        if (!collData.releases || collData.releases.length === 0) continue;

        const instance = collData.releases[0];
        const info = instance.basic_information || {};

        // Log completo per debug
        console.log(`💿 Discogs instance keys: ${Object.keys(instance).join(', ')}`);
        console.log(`💿 Discogs instance.notes raw: ${JSON.stringify(instance.notes)}`);
        console.log(`💿 Discogs instance.rating: ${instance.rating}`);
        console.log(`💿 Discogs instance.folder_id: ${instance.folder_id}`);

        // Mappa note con nomi dei campi personalizzati
        const rawNotes = instance.notes || [];
        const notes = rawNotes
          .filter(n => n.value !== undefined && n.value !== null && String(n.value).trim() !== '')
          .map(n => ({
            fieldId: n.field_id,
            fieldName: customFields[n.field_id] || `Campo ${n.field_id}`,
            value: String(n.value).trim()
          }));
        console.log(`💿 Discogs notes mapped: ${JSON.stringify(notes)}`);

        const result = {
          found: true,
          inCollection: true,
          releaseId: release.id,
          title: info.title || release.title || '',
          artist: info.artists?.map(a => a.name).join(', ') || '',
          year: info.year || release.year || '',
          label: info.labels?.map(l => l.name).join(', ') || '',
          catno: info.labels?.[0]?.catno || '',
          format: info.formats?.map(f => `${f.name} ${(f.descriptions || []).join(', ')}`).join(' / ') || '',
          cover: release.cover_image || info.cover_image || '',
          discogsUrl: `https://www.discogs.com/release/${release.id}`,
          notes,
          rating: instance.rating || 0
        };
        console.log(`💿 Discogs: "${result.title}" IN COLLEZIONE (${result.label}, ${result.year}, rating: ${result.rating}, notes: ${notes.length})`);
        return res.json(result);
      } catch (e) {
        console.warn(`⚠️ Discogs collection check error for ${release.id}:`, e.message);
        continue;
      }
    }

    // Non in collezione ma trovato su Discogs
    const first = searchData.results[0];
    console.log(`💿 Discogs [${searchLabel}]: "${first.title}" trovato ma NON in collezione`);
    res.json({
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

  } catch (err) {
    console.error('❌ Discogs error:', err.message);
    res.json({ found: false, error: err.message });
  }
});

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
