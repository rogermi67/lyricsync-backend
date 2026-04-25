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

// Temporary storage for request tokens (in memory, short-lived)
const pendingOAuthTokens = {};

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

    // Salva temporaneamente il token secret (serve per step 3)
    pendingOAuthTokens[oauthToken] = { secret: oauthTokenSecret, ts: Date.now() };
    // Pulisci token vecchi (>10 minuti)
    const now = Date.now();
    for (const [k, v] of Object.entries(pendingOAuthTokens)) {
      if (now - v.ts > 600000) delete pendingOAuthTokens[k];
    }

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

    const pending = pendingOAuthTokens[oauth_token];
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
    delete pendingOAuthTokens[oauth_token];

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
    res.json({ authorized: true, authorizedAt: tokens.authorizedAt || '' });
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
    const artistLower = artist.toLowerCase().replace(/[^a-z0-9]/g, '');

    // Cerca release nella collezione il cui artista corrisponde
    // L'includes è permesso solo se la stringa più corta è almeno il 60% di quella più lunga
    // per evitare falsi positivi tipo "Heart" dentro "Tom Petty & The Heartbreakers"
    const matches = collection.filter(r => {
      return r.artists.some(a => {
        const aLower = a.toLowerCase().replace(/[^a-z0-9]/g, '');
        if (aLower === artistLower) return true;
        const shorter = aLower.length < artistLower.length ? aLower : artistLower;
        const longer = aLower.length < artistLower.length ? artistLower : aLower;
        if (shorter.length < 4) return false; // nomi troppo corti: solo match esatto
        if (shorter.length / longer.length < 0.5) return false; // troppo diversi in lunghezza
        return longer.includes(shorter);
      });
    });

    if (matches.length > 0) {
      console.log(`💿 Discogs: ${matches.length} release di "${artist}" nella tua collezione`);

      // Se c'è un solo match, è quello
      // Se ce ne sono più di uno, prova a trovare quello il cui titolo corrisponde all'album Shazam
      let best = matches[0];
      if (matches.length > 1 && album) {
        const albumLower = album.toLowerCase().replace(/[^a-z0-9]/g, '');
        const albumMatch = matches.find(r => {
          const tLower = r.title.toLowerCase().replace(/[^a-z0-9]/g, '');
          return tLower === albumLower || tLower.includes(albumLower) || albumLower.includes(tLower);
        });
        if (albumMatch) best = albumMatch;
      }

      // Mappa le note con nomi campi
      const notes = (best.notes || [])
        .filter(n => n.value !== undefined && n.value !== null && String(n.value).trim() !== '')
        .map(n => ({
          fieldId: n.field_id,
          fieldName: customFields[n.field_id] || `Campo ${n.field_id}`,
          value: String(n.value).trim()
        }));

      // Recupera tracklist dalla release Discogs (per pre-fetch testi)
      let tracklist = [];
      try {
        const authHeader = `Discogs key=${cfg.consumerKey}, secret=${cfg.consumerSecret}`;
        const releaseRes = await fetch(`https://api.discogs.com/releases/${best.id}`, {
          headers: { 'Authorization': authHeader, 'User-Agent': 'LyricSync/1.0' }
        });
        if (releaseRes.ok) {
          const releaseData = await releaseRes.json();
          tracklist = (releaseData.tracklist || [])
            .filter(t => t.type_ === 'track') // escludi headings e subheadings
            .map(t => ({
              position: t.position || '',
              title: t.title || '',
              duration: t.duration || ''
            }));
          console.log(`💿 Discogs tracklist: ${tracklist.length} tracce`);
        }
      } catch (e) { console.warn('⚠️ Discogs tracklist error:', e.message); }

      const result = {
        found: true,
        inCollection: true,
        releaseId: best.id,
        instanceId: best.instanceId,
        folderId: best.folderId || 0,
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
      console.log(`💿 Discogs: "${result.title}" IN COLLEZIONE (${result.label}, ${result.year}, notes: ${notes.length}, tracks: ${tracklist.length})`);
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
      if (data.results && data.results.length > 0) {
        const first = data.results[0];
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

    const folder = folderId || 0;
    const url = `https://api.discogs.com/users/${cfg.username}/collection/folders/${folder}/releases/${releaseId}/instances/${instanceId}/fields/${fieldId}`;

    // Firma la richiesta con OAuth 1.0a
    const oauth = createOAuthClient(cfg);
    const requestData = { url, method: 'POST' };
    const token = { key: oauthTokens.accessToken, secret: oauthTokens.accessTokenSecret };
    const oauthHeader = oauth.toHeader(oauth.authorize(requestData, token));

    const apiRes = await fetch(url, {
      method: 'POST',
      headers: {
        ...oauthHeader,
        'User-Agent': 'LyricSync/1.0',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ value: String(value) })
    });

    if (apiRes.status === 204 || apiRes.ok) {
      console.log(`💿 Discogs: campo ${fieldId} aggiornato per release ${releaseId} → "${value}"`);
      return res.json({ success: true });
    }

    const errText = await apiRes.text();
    if (apiRes.status === 401) {
      console.warn(`⚠️ Discogs update field 401: token OAuth scaduto o revocato`);
      return res.json({ success: false, reason: 'oauth_expired', message: 'Token OAuth scaduto. Riautorizza Discogs nelle impostazioni.' });
    }
    if (apiRes.status === 403) {
      console.warn(`⚠️ Discogs update field 403: ${errText}`);
      return res.json({ success: false, reason: 'oauth_required', message: 'Errore autorizzazione Discogs. Riprova l\'autorizzazione OAuth.' });
    }
    console.error(`❌ Discogs update field error: ${apiRes.status} ${errText}`);
    res.status(apiRes.status).json({ error: `Discogs API error: ${apiRes.status}`, details: errText });

  } catch (err) {
    console.error('❌ Discogs update field error:', err.message);
    res.status(500).json({ error: err.message });
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
