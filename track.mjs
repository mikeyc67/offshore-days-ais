// Offshore Days Tracker — hourly AIS check.
// For every user who has switched tracking on in the app, listen briefly to aisstream.io for their
// ship, work out whether it is in a port and which side of the UK 12-mile line it is on, and record
// any change (left port, left/entered UK waters, arrived) in their Firebase log for the app to show.
import { initializeApp, cert } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';
import { readFileSync } from 'node:fs';

const UK = JSON.parse(readFileSync(new URL('./uk12.json', import.meta.url)));
const PORTS = JSON.parse(readFileSync(new URL('./ports.json', import.meta.url)));
const KEY = process.env.AISSTREAM_KEY;
const LISTEN_MS = Number(process.env.LISTEN_SECONDS || 150) * 1000;
const PORT_RADIUS_NM = 4;

// never let one odd message end the whole run
process.on('unhandledRejection', e => console.error('Ignored an unexpected error:', e?.message || e));
process.on('uncaughtException', e => console.error('Ignored an unexpected error:', e?.message || e));

if (!KEY) throw new Error('AISSTREAM_KEY secret is missing (GitHub → Settings → Secrets and variables → Actions).');
if (!process.env.FIREBASE_SERVICE_ACCOUNT) throw new Error('FIREBASE_SERVICE_ACCOUNT secret is missing.');
initializeApp({ credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)) });
const db = getFirestore();

// ---- geography ----
function inRing(r, x, y) {
  let c = false;
  for (let i = 0, j = r.length - 1; i < r.length; j = i++) {
    const [xi, yi] = r[i], [xj, yj] = r[j];
    if ((yi > y) !== (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi) c = !c;
  }
  return c;
}
const inUK = (lat, lon) => UK.some(r => inRing(r, lon, lat));
function distNm(lat1, lon1, lat2, lon2) {
  const R = 3440.065, toRad = d => d * Math.PI / 180;
  const a = Math.sin(toRad(lat2 - lat1) / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(toRad(lon2 - lon1) / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(a));
}
// In a port = stopped (or moored / at anchor) within a few miles of a known port.
function portAt(lat, lon, sog, nav) {
  const stopped = (sog ?? 99) < 1 || nav === 1 || nav === 5;
  if (!stopped) return null;
  let best = null;
  for (const p of PORTS) {
    const d = distNm(lat, lon, p.lat, p.lon);
    if (d <= PORT_RADIUS_NM && (!best || d < best.d)) best = { ...p, d };
  }
  return best;
}
// "2026-09-25 10:12:33.123456789 +0000 UTC" → ISO
function aisTime(s) {
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/.exec(String(s || ''));
  return m ? `${m[1]}T${m[2]}Z` : null;
}

// ---- listen to aisstream for these MMSIs until each has reported a position, or time runs out ----
const seen = new Set();   // message types received, for the log
let refused = '';          // aisstream's reason, if it turned the connection down
function listen(mmsis) {
  return new Promise(resolve => {
    const got = new Map();
    let finished = false;
    const finish = () => { if (finished) return; finished = true; clearTimeout(timer); try { ws.close(); } catch {} resolve(got); };
    const timer = setTimeout(finish, LISTEN_MS);
    const ws = new WebSocket('wss://stream.aisstream.io/v0/stream');
    ws.onopen = () => ws.send(JSON.stringify({
      APIKey: KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FiltersShipMMSI: mmsis,
      FilterMessageTypes: ['PositionReport', 'StandardClassBPositionReport', 'ShipStaticData'],
    }));
    ws.onmessage = async ev => {
      try {
        const text = typeof ev.data === 'string' ? ev.data : await new Response(ev.data).text();
        let m; try { m = JSON.parse(text); } catch { return; }
        if (m.error) { refused = String(m.error); console.error('aisstream refused the connection:', m.error); return finish(); }
        seen.add(m.MessageType || '?');
        const mmsi = String(m.MetaData?.MMSI ?? m.MetaData?.MMSI_String ?? '');
        const g = got.get(mmsi) || {};
        // the report itself is usually under Message[MessageType]; MetaData also carries a latitude/longitude
        const body = m.Message?.[m.MessageType] ?? Object.values(m.Message || {})[0] ?? {};
        if (typeof body.Destination === 'string') g.dest = body.Destination.trim();
        const lat = typeof body.Latitude === 'number' ? body.Latitude : m.MetaData?.latitude;
        const lon = typeof body.Longitude === 'number' ? body.Longitude : m.MetaData?.longitude;
        // 91 / 181 mean "position not available" in AIS
        if (m.MessageType !== 'ShipStaticData' && typeof lat === 'number' && typeof lon === 'number' && Math.abs(lat) <= 90 && Math.abs(lon) <= 180 && !(lat === 0 && lon === 0)) {
          g.pos = { lat, lon, sog: typeof body.Sog === 'number' ? body.Sog : null, nav: body.NavigationalStatus ?? null, t: aisTime(m.MetaData?.time_utc) };
        }
        got.set(mmsi, g);
        if (mmsis.every(x => got.get(x)?.pos)) finish();
      } catch (e) {
        console.error('Skipped an AIS message that could not be read:', e.message);
      }
    };
    ws.onerror = e => console.error('Connection problem:', e?.message || e);
    ws.onclose = finish;
  });
}

// ---- main ----
const now = new Date().toISOString();
const tracks = [];
for (const user of await db.collection('users').listDocuments()) {
  const snap = await user.collection('aistrack').doc('current').get();
  const cfg = snap.exists ? snap.data() : null;
  if (cfg?.active && /^\d{9}$/.test(String(cfg.mmsi || ''))) tracks.push({ user, cfg: { ...cfg, mmsi: String(cfg.mmsi) } });
}
if (!tracks.length) { console.log('Nobody has tracking switched on. Nothing to do.'); process.exit(0); }

const mmsis = [...new Set(tracks.map(t => t.cfg.mmsi))];
console.log(`Listening up to ${LISTEN_MS / 1000}s for ${mmsis.length} vessel(s)…`);
let got = new Map(), listenError = '';
try { got = await listen(mmsis); } catch (e) { listenError = e.message || String(e); console.error('Could not listen to aisstream:', listenError); }
if (seen.size) console.log('AIS message types received:', [...seen].join(', '));
// a problem the app should show: aisstream refusing the key, or the connection failing outright
const problem = refused ? `aisstream refused the connection: ${refused}` : listenError ? `Could not reach aisstream: ${listenError}` : '';

const ORDER = { depart: 0, leave12: 1, enter12: 2, arrive: 3 };
for (const { user, cfg } of tracks) {
  const stateRef = user.collection('aistrack').doc('state');
  try {
  const prev = (await stateRef.get()).data() || {};
  const g = got.get(cfg.mmsi);
  const update = { id: 'state', lastRun: now, mmsi: cfg.mmsi, lastError: problem ? { t: now, message: problem } : null };
  if (!g?.pos || typeof g.pos.lat !== 'number') {
    update.lastNoSignal = now;
    await stateRef.set(update, { merge: true });
    console.log(`${cfg.vesselName || cfg.mmsi}: no AIS position this hour (normal well offshore).`);
    continue;
  }
  const { lat, lon, sog, nav } = g.pos;
  const port = portAt(lat, lon, sog, nav);
  const fix = {
    t: g.pos.t || now, lat, lon, sog, nav,
    inUK: inUK(lat, lon),
    port: port ? port.name : null, portUK: port ? !!port.uk : null,
    dest: g.dest || prev.fix?.dest || '',
  };
  const last = prev.mmsi === cfg.mmsi ? prev.fix : null;   // a new ship starts fresh
  const events = [];
  if (last) {
    if (last.port && last.port !== fix.port) events.push({ type: 'depart', port: last.port, portUK: last.portUK, after: last.t, t: fix.t, lat: last.lat, lon: last.lon });
    if (last.inUK !== fix.inUK) events.push({ type: fix.inUK ? 'enter12' : 'leave12', after: last.t, t: fix.t, lat, lon });
    if (fix.port && last.port !== fix.port) events.push({ type: 'arrive', port: fix.port, portUK: fix.portUK, after: last.t, t: fix.t, lat, lon });
  }
  events.sort((a, b) => ORDER[a.type] - ORDER[b.type]);
  for (const [i, e] of events.entries()) {
    const id = `${Date.parse(e.t) || Date.now()}-${i}-${e.type}`;
    await user.collection('aisevents').doc(id).set({ id, ...e, vessel: cfg.vesselName || '', mmsi: cfg.mmsi, status: 'new' });
  }
  update.fix = fix;
  await stateRef.set(update, { merge: true });
  console.log(`${cfg.vesselName || cfg.mmsi}: ${lat.toFixed(4)}, ${lon.toFixed(4)} · ${fix.inUK ? 'inside' : 'outside'} UK 12 nm${fix.port ? ' · in ' + fix.port : ''}${events.length ? ' · events: ' + events.map(e => e.type).join(', ') : ''}`);
  } catch (e) {
    // keep going for other users, and leave a note the app can show
    console.error(`${cfg.vesselName || cfg.mmsi}: ${e.stack || e}`);
    try { await stateRef.set({ id: 'state', lastRun: now, lastError: { t: now, message: String(e.message || e) } }, { merge: true }); } catch {}
  }
}
if (problem) console.error(problem);
process.exit(0);
