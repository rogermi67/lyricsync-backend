// Test di integrazione: stubba Redis, lrclib e Discogs, poi verifica
// /lyrics (cache) e /lyrics/prefetch (job in background).
const Module = require('module');
const origRequire = Module.prototype.require;

// ─── Fake Redis in memoria ───────────────────────────────────────────────────
const store = new Map();
let redisGets = 0, redisSets = 0;
class FakeRedis {
  constructor() {}
  async get(k) { redisGets++; return store.has(k) ? store.get(k) : null; }
  async set(k, v) { redisSets++; store.set(k, v); return 'OK'; }
  async del(k) { store.delete(k); return 1; }
  async expire() { return 1; }
}

// ─── Fake fetch (lrclib + Discogs) ───────────────────────────────────────────
const calls = { lrclibGet: 0, lrclibSearch: 0, discogsRelease: 0, discogsSearch: 0 };
const ALBUM_TRACKS = [
  { position: 'A1', title: 'Gimme Shelter', type_: 'track', duration: '4:31' },
  { position: 'A2', title: 'Love In Vain', type_: 'track', duration: '4:19' },
  { position: 'A3', title: 'Country Honk', type_: 'track', duration: '3:09' },
  { position: 'B1', title: 'Midnight Rambler', type_: 'track', duration: '6:52' },
  { position: 'B2', title: 'You Got The Silver', type_: 'track', duration: '2:51' },
  { position: 'B3', title: 'Monkey Man', type_: 'track', duration: '4:12' },
  { position: '', title: 'Side B', type_: 'heading' },              // va scartata
  { position: 'B4', title: 'You Cant Always Get What You Want', type_: 'track', duration: '7:28' },
];
const NO_LYRICS = new Set(['country honk']);

function jsonRes(body, status = 200) {
  return { ok: status >= 200 && status < 300, status, json: async () => body, text: async () => JSON.stringify(body) };
}

async function fakeFetch(url) {
  const u = String(url);
  if (u.includes('lrclib.net/api/get')) {
    calls.lrclibGet++;
    const track = decodeURIComponent(new URL(u).searchParams.get('track_name') || '').toLowerCase();
    if (NO_LYRICS.has(track)) return jsonRes({ error: 'not found' }, 404);
    await new Promise(r => setTimeout(r, 30));  // simula latenza di rete
    return jsonRes({ syncedLyrics: `[00:12.00] ${track} riga 1`, plainLyrics: `${track} riga 1` });
  }
  if (u.includes('lrclib.net/api/search')) {
    calls.lrclibSearch++;
    return jsonRes([]);  // nessun fallback trovato
  }
  if (u.includes('api.discogs.com/releases/')) {
    calls.discogsRelease++;
    return jsonRes({ tracklist: ALBUM_TRACKS, images: [] });
  }
  if (u.includes('api.discogs.com/database/search')) {
    calls.discogsSearch++;
    return jsonRes({ results: [{ id: 12345, title: 'The Rolling Stones - Let It Bleed' }] });
  }
  if (u.includes('api.discogs.com/users/')) {
    return jsonRes({ pagination: { pages: 1 }, releases: [] });  // collezione vuota
  }
  return jsonRes({}, 404);
}

// ─── Intercetta i require ────────────────────────────────────────────────────
Module.prototype.require = function (name) {
  if (name === '@upstash/redis') return { Redis: FakeRedis };
  if (name === 'node-fetch') return fakeFetch;
  if (name === 'ffmpeg-static') return '/usr/bin/ffmpeg';
  if (name === 'fluent-ffmpeg') { const f = () => f; f.setFfmpegPath = () => {}; return f; }
  return origRequire.apply(this, arguments);
};

process.env.PORT = '3999';
process.env.APP_PASSWORD = '';               // niente auth durante il test
process.env.UPSTASH_REDIS_URL = 'http://fake';
process.env.UPSTASH_REDIS_TOKEN = 'fake';
process.env.RAPIDAPI_KEY = 'testkey123456';

// Config Discogs pre-caricata in Redis
store.set('lyricsync:discogs', { username: 'tester', consumerKey: 'ck', consumerSecret: 'cs' });

require('./server.js');

// ─── Test ────────────────────────────────────────────────────────────────────
const BASE = 'http://localhost:3999';
let failures = 0;
function check(label, cond, extra = '') {
  console.log(`${cond ? '  ✅' : '  ❌'} ${label}${extra ? ' — ' + extra : ''}`);
  if (!cond) failures++;
}
const get = async (path) => (await fetch(BASE + path)).json();

(async () => {
  await new Promise(r => setTimeout(r, 400));
  console.log('\n─── TEST 1: /lyrics usa la cache Redis ───');

  const first = await get('/lyrics?title=Gimme%20Shelter&artist=The%20Rolling%20Stones');
  check('primo giro: testo trovato', first.found === true);
  check('primo giro: NON da cache', first.cached === false);
  check('primo giro: ha il testo sincronizzato', !!first.syncedLyrics);
  const lrclibAfterFirst = calls.lrclibGet;

  const second = await get('/lyrics?title=Gimme%20Shelter&artist=The%20Rolling%20Stones');
  check('secondo giro: servito da cache', second.cached === true);
  check('secondo giro: nessuna nuova chiamata a lrclib', calls.lrclibGet === lrclibAfterFirst,
    `lrclib get: ${calls.lrclibGet}`);
  check('chiave Redis creata', [...store.keys()].some(k => k.startsWith('lyricsync:lyrics:the-rolling-stones:gimme-shelter')),
    [...store.keys()].filter(k => k.startsWith('lyricsync:lyrics')).join(', '));

  console.log('\n─── TEST 2: chiave normalizzata (accenti, maiuscole, parentesi) ───');
  await get('/lyrics?title=Sympathy%20For%20The%20Devil%20(Remastered)&artist=THE%20ROLLING%20STONES');
  const k = [...store.keys()].filter(x => x.includes('sympathy'));
  check('titolo normalizzato senza "(Remastered)"', k.length === 1 && k[0] === 'lyricsync:lyrics:the-rolling-stones:sympathy-for-the-devil', k.join(','));

  console.log('\n─── TEST 3: /lyrics/prefetch avvia il job e risponde subito ───');
  const t0 = Date.now();
  const start = await get('/lyrics/prefetch?artist=The%20Rolling%20Stones&album=Let%20It%20Bleed&releaseId=12345');
  const elapsed = Date.now() - t0;
  check('risposta immediata (< 250ms)', elapsed < 250, `${elapsed}ms`);
  check('job avviato', start.running === true);
  check('7 tracce (heading escluso)', start.total === 7, `total=${start.total}`);
  check('tutte pending all\'avvio', start.tracks.every(t => t.status === 'pending'));
  check('releaseId propagato', String(start.releaseId) === '12345');

  console.log('\n─── TEST 4: il job avanza e completa ───');
  let status = null;
  for (let i = 0; i < 40; i++) {
    await new Promise(r => setTimeout(r, 100));
    status = await get('/lyrics/prefetch/status?artist=The%20Rolling%20Stones&album=Let%20It%20Bleed');
    if (!status.running) break;
  }
  check('job completato', status && status.running === false);
  check('done == total', status.done === status.total, `${status.done}/${status.total}`);
  const ready = status.tracks.filter(t => t.status === 'ready');
  const missing = status.tracks.filter(t => t.status === 'missing');
  check('6 testi pronti', ready.length === 6, `ready=${ready.length}`);
  check('1 testo mancante (Country Honk)', missing.length === 1 && missing[0].title === 'Country Honk',
    missing.map(t => t.title).join(','));
  check('flag synced impostato', ready.every(t => t.synced === true));
  check('testi salvati in Redis', [...store.keys()].filter(x => x.startsWith('lyricsync:lyrics:')).length >= 8,
    `${[...store.keys()].filter(x => x.startsWith('lyricsync:lyrics:')).length} chiavi`);

  console.log('\n─── TEST 5: un testo pre-fetchato ora arriva dalla cache ───');
  const lrclibBefore = calls.lrclibGet;
  const cachedTrack = await get('/lyrics?title=Monkey%20Man&artist=The%20Rolling%20Stones');
  check('servito da cache', cachedTrack.cached === true);
  check('nessuna chiamata a lrclib', calls.lrclibGet === lrclibBefore);

  console.log('\n─── TEST 6: secondo prefetch dello stesso album non rifà il lavoro ───');
  const discogsBefore = calls.discogsRelease;
  const again = await get('/lyrics/prefetch?artist=The%20Rolling%20Stones&album=Let%20It%20Bleed&releaseId=12345');
  check('job riusato (resumed)', again.resumed === true);
  check('nessuna nuova chiamata a Discogs', calls.discogsRelease === discogsBefore);

  console.log('\n─── TEST 7: album senza releaseId → risolto via Discogs ───');
  const noId = await get('/lyrics/prefetch?artist=Pink%20Floyd&album=Animals');
  check('tracklist risolta tramite ricerca Discogs', noId.total === 7, `total=${noId.total}`);
  check('ricerca database Discogs usata', calls.discogsSearch >= 1);

  console.log('\n─── TEST 8: parametri mancanti ───');
  const bad = await fetch(BASE + '/lyrics/prefetch?artist=X');
  check('400 senza album', bad.status === 400);
  const noJob = await get('/lyrics/prefetch/status?artist=Nessuno&album=Niente');
  check('status di un job inesistente', noJob.found === false);

  console.log(`\n${failures === 0 ? '🎉 TUTTI I TEST PASSATI' : `💥 ${failures} TEST FALLITI`}`);
  console.log(`   chiamate: lrclib get=${calls.lrclibGet}, lrclib search=${calls.lrclibSearch}, discogs release=${calls.discogsRelease}, discogs search=${calls.discogsSearch}`);
  console.log(`   redis: ${redisGets} get, ${redisSets} set\n`);
  process.exit(failures === 0 ? 0 : 1);
})();
