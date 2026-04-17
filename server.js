require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const fetch = require('node-fetch');
const fs = require('fs');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegPath = require('ffmpeg-static');

ffmpeg.setFfmpegPath(ffmpegPath);

const app = express();
if (!fs.existsSync('uploads')) fs.mkdirSync('uploads');

const upload = multer({ dest: 'uploads/' });
app.use(cors());
app.use(express.json());

const COUNTER_FILE = 'calls_counter.json';
const TOTAL_FREE = 500;

function loadCounter() {
  try { if (fs.existsSync(COUNTER_FILE)) return JSON.parse(fs.readFileSync(COUNTER_FILE, 'utf8')); } catch {}
  return { used: 0 };
}
function saveCounter(data) { fs.writeFileSync(COUNTER_FILE, JSON.stringify(data)); }

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

    const counter = loadCounter();
    counter.used += 1;
    saveCounter(counter);
    console.log(`📊 Chiamata #${counter.used} (rimaste: ${TOTAL_FREE - counter.used})`);

    convertedPath = await convertToWav(req.file.path);
    const audioData = fs.readFileSync(convertedPath);
    const pcmData = audioData.slice(44);
    const base64Audio = pcmData.toString('base64');

    const response = await fetch('https://shazam.p.rapidapi.com/songs/v2/detect', {
      method: 'POST',
      headers: {
        'content-type': 'text/plain',
        'X-RapidAPI-Key': process.env.RAPIDAPI_KEY,
        'X-RapidAPI-Host': 'shazam.p.rapidapi.com'
      },
      body: base64Audio
    });

    const data = await response.json();
    if (!data?.track) return res.json({ found: false });

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
    const { text, targetLang = 'it' } = req.body;
    if (!text) return res.status(400).json({ error: 'Testo mancante' });

    // Dividi il testo in righe, traduci in blocco
    const sourceLang = 'en'; // la maggior parte dei testi è in inglese
    const url = `https://api.mymemory.translated.net/get?q=${encodeURIComponent(text.substring(0, 5000))}&langpair=${sourceLang}|${targetLang}&de=rogermi@gmail.com`;

    const response = await fetch(url);
    const data = await response.json();

    if (data.responseStatus === 200 && data.responseData?.translatedText) {
      res.json({
        translated: data.responseData.translatedText,
        sourceLang,
        targetLang
      });
    } else {
      res.json({ error: 'Traduzione non disponibile' });
    }
  } catch (err) {
    console.error('❌ Errore translate:', err.message);
    res.status(500).json({ error: 'Errore traduzione' });
  }
});

app.get('/counter', (req, res) => {
  const counter = loadCounter();
  res.json({ used: counter.used, remaining: TOTAL_FREE - counter.used, total: TOTAL_FREE });
});

app.get('/', (req, res) => res.json({ status: 'LyricSync backend attivo ✅' }));

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  const c = loadCounter();
  console.log(`✅ LyricSync backend attivo su http://localhost:${PORT}`);
  console.log(`📊 Chiamate usate: ${c.used}/${TOTAL_FREE} (rimaste: ${TOTAL_FREE - c.used})`);
});
