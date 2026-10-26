// Serverside Tracker collector. Runs every 30 min in GitHub Actions (free), writes small JSON files to data/.
//   node collector/collect.mjs [--stream path/to/stream.bin] [--top 300] [--out data]
// Source: Cfx's public server list (frontend.cfx-services.net). The bulk stream gives every listed server
// (players, name, locale, tags, build, premium); scripts + owners come from per-server lookups of the top N.
import fs from 'node:fs';
import path from 'node:path';

const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 ? process.argv[i + 1] : d; };
const OUT = arg('--out', 'data');
const TOP_ENRICH = Number(arg('--top', '300'));
const STREAM_FILE = arg('--stream', null);
const UA = { 'User-Agent': 'ServersideTracker/1.0 (+https://serversidedev.com/tracker)' };
const BASE = 'https://frontend.cfx-services.net/api/servers';

// ---------- protobuf (just enough for the Cfx server list) ----------
function varint(b, p) { let x = 0, s = 0, c; do { c = b[p++]; x += (c & 0x7f) * 2 ** s; s += 7; } while (c & 0x80); return [x, p]; }
function* fields(b, p, end) {
  while (p < end) {
    let k; [k, p] = varint(b, p); const fn = Math.floor(k / 8), wt = k % 8;
    if (wt === 0) { let v; [v, p] = varint(b, p); yield { fn, wt, v }; }
    else if (wt === 2) { let l; [l, p] = varint(b, p); yield { fn, wt, s: p, e: p + l }; p += l; }
    else if (wt === 5) p += 4; else if (wt === 1) p += 8; else throw new Error('wire type ' + wt);
  }
}
const str = (b, f) => b.toString('utf8', f.s, f.e);

export function decodeStream(buf) {
  const servers = []; let p = 0;
  while (p + 4 <= buf.length) {
    const len = buf.readUInt32LE(p); p += 4; const end = p + len;
    const s = { code: '', max: 0, players: 0, hostname: '', gametype: '', mapname: '', vars: {} };
    for (const f of fields(buf, p, end)) {
      if (f.fn === 1) s.code = str(buf, f);
      else if (f.fn === 2) for (const g of fields(buf, f.s, f.e)) {
        if (g.fn === 1) s.max = g.v; else if (g.fn === 2) s.players = g.v;
        else if (g.fn === 4) s.hostname = str(buf, g); else if (g.fn === 5) s.gametype = str(buf, g);
        else if (g.fn === 6) s.mapname = str(buf, g); else if (g.fn === 17) s.upvote = g.v;
        else if (g.fn === 12) { let k = '', v = ''; for (const h of fields(buf, g.s, g.e)) { if (h.fn === 1) k = str(buf, h); else if (h.fn === 2) v = str(buf, h); } if (k) s.vars[k] = v; }
      }
    }
    servers.push(s); p = end;
  }
  return servers;
}

// ---------- helpers ----------
export const clean = (t) => String(t || '').replace(/\^[0-9]/g, '').replace(/~[a-zA-Z]~/g, '').replace(/\s+/g, ' ').trim();
const country = (loc) => { const m = /^[a-z]{2,3}-([A-Z]{2})$/.exec(loc || ''); return m ? m[1] : null; };
const FRAMEWORKS = [
  ['ESX', /^es_extended$/i], ['QBCore', /^qb-core$/i], ['Qbox', /^qbx_core$/i], ['ox_core', /^ox_core$/i],
  ['vRP', /^vrp$/i], ['ND Core', /^nd_core$/i], ['Creative', /^creative/i],
];
const frameworkOf = (resources) => { for (const [name, re] of FRAMEWORKS) if (resources.some((r) => re.test(r))) return name; return resources.length ? 'Standalone / other' : null; };

// Cfx rate-limits parallel lookups (8 at once lost 60%); 3 at once with a back-off on 429 gets them all.
async function getJSON(url, tries = 5) {
  for (let i = 0; i <= tries; i++) {
    let wait = 1000 * (i + 1);
    try {
      const r = await fetch(url, { headers: UA, signal: AbortSignal.timeout(15000) });
      if (r.ok) return await r.json();
      if (r.status === 404) return null;
      if (r.status === 429) wait = Math.max(wait, (Number(r.headers.get('retry-after')) || 3) * 1000);
    } catch {}
    await new Promise((res) => setTimeout(res, wait));
  }
  return null;
}
// FiveM's built-in resources: every server has them, so they are hidden from the script rankings.
const BUILTIN = new Set(['hardcap', 'monitor', 'sessionmanager', 'spawnmanager', 'mapmanager', 'chat', 'basic-gamemode', 'fivem', 'fivem-map-skater',
  'fivem-map-hipster', 'webadmin', 'yarn', 'webpack', 'rconlog', 'playernames', 'baseevents', 'runcode', 'sessionmanager-rdr3', 'redm-map-one', 'chat-theme-gtao',
  'example-loadscreen', 'money', 'money-fountain', 'money-fountain-example-map', 'ped-money-drops', 'player-data', 'txAdmin', 'txadmin']);
async function pool(items, n, fn) { const out = new Array(items.length); let i = 0; await Promise.all(Array.from({ length: n }, async () => { while (i < items.length) { const k = i++; out[k] = await fn(items[k]); } })); return out; }

// ---------- main ----------
async function main() {
  const t0 = Date.now();
  const buf = STREAM_FILE ? fs.readFileSync(STREAM_FILE)
    : Buffer.from(await (await fetch(BASE + '/streamRedir/', { headers: UA, signal: AbortSignal.timeout(120000) })).arrayBuffer());
  const all = decodeStream(buf).filter((s) => s.code && s.vars.gamename !== 'rdr3');
  const live = all.filter((s) => s.players > 0);
  const players = live.reduce((a, s) => a + s.players, 0);

  const count = (arr, keyFn, weightFn = () => 1) => { const m = {}; for (const s of arr) { const k = keyFn(s); if (k) m[k] = (m[k] || 0) + weightFn(s); } return m; };
  const top = (m, n) => Object.entries(m).sort((a, b) => b[1] - a[1]).slice(0, n);

  const byCountryPlayers = count(live, (s) => country(s.vars.locale), (s) => s.players);
  const byCountryServers = count(live, (s) => country(s.vars.locale));
  const byBuild = count(live, (s) => s.vars.sv_enforceGameBuild || 'default', (s) => s.players);
  const byPremium = count(live, (s) => s.vars.premium || 'none');
  const tagCount = {}; for (const s of live) for (const t of String(s.vars.tags || '').split(',').map((x) => x.trim().toLowerCase()).filter((x) => x && x.length < 30)) tagCount[t] = (tagCount[t] || 0) + 1;

  const ranked = [...live].sort((a, b) => b.players - a.players);
  const enrichList = ranked.slice(0, TOP_ENRICH);
  const details = await pool(enrichList, 3, (s) => getJSON(`${BASE}/single/${s.code}`));

  const resServers = {}, resPlayers = {}, fwServers = {}, fwPlayers = {}, owners = {};
  enrichList.forEach((s, i) => {
    const d = details[i]?.Data; if (!d) return;
    const res = [...new Set((d.resources || []).map((r) => String(r)))];
    s.resources = res.length; s.framework = frameworkOf(res);
    for (const r of res) { if (BUILTIN.has(r)) continue; resServers[r] = (resServers[r] || 0) + 1; resPlayers[r] = (resPlayers[r] || 0) + s.players; }
    if (s.framework) { fwServers[s.framework] = (fwServers[s.framework] || 0) + 1; fwPlayers[s.framework] = (fwPlayers[s.framework] || 0) + s.players; }
    if (d.ownerID) { const o = owners[d.ownerID] || (owners[d.ownerID] = { name: clean(d.ownerName) || 'unknown', servers: 0, players: 0, top: null }); o.servers++; o.players += s.players; if (!o.top || s.players > o.top.players) o.top = { code: s.code, name: clean(s.vars.sv_projectName || s.hostname), players: s.players }; }
  });
  const enriched = details.filter(Boolean).length;

  const serverRow = (s, rank) => ({
    rank, code: s.code, name: clean(s.vars.sv_projectName || s.hostname).slice(0, 90), desc: clean(s.vars.sv_projectDesc).slice(0, 140),
    players: s.players, max: s.max, peak: Number(s.vars.peak_players) || null, country: country(s.vars.locale), locale: s.vars.locale || null,
    tags: String(s.vars.tags || '').split(',').map((x) => x.trim()).filter(Boolean).slice(0, 5), build: s.vars.sv_enforceGameBuild || null,
    premium: s.vars.premium || null, framework: s.framework || null, icon: s.vars.icon_url || null,
  });

  const now = new Date().toISOString();
  const summary = {
    generated: now, source: 'Cfx.re public server list', refreshMinutes: 30,
    totals: { listed: all.length, online: live.length, players, enriched, enrichTarget: enrichList.length },
    topServers: ranked.slice(0, 100).map((s, i) => serverRow(s, i + 1)),
    countries: top(byCountryPlayers, 40).map(([c, p]) => ({ country: c, players: p, servers: byCountryServers[c] || 0 })),
    builds: top(byBuild, 12).map(([b, p]) => ({ build: b, players: p })),
    premium: top(byPremium, 6).map(([k, n]) => ({ tier: k, servers: n })),
    tags: top(tagCount, 30).map(([t, n]) => ({ tag: t, servers: n })),
    resources: top(resServers, 60).map(([r, n]) => ({ name: r, servers: n, share: +(n / Math.max(1, enriched)).toFixed(3), players: resPlayers[r] })),
    frameworks: top(fwServers, 10).map(([f, n]) => ({ name: f, servers: n, players: fwPlayers[f], share: +(n / Math.max(1, enriched)).toFixed(3) })),
    owners: Object.values(owners).sort((a, b) => b.players - a.players).slice(0, 40),
    note: `Scripts, frameworks and owners are measured across the top ${enrichList.length} servers by players.`,
  };

  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, 'summary.json'), JSON.stringify(summary));
  // history: one point per run, 7 days kept
  const hf = path.join(OUT, 'history.json');
  let hist = []; try { hist = JSON.parse(fs.readFileSync(hf, 'utf8')); } catch {}
  hist.push({ t: now, players, online: live.length });
  const cutoff = Date.now() - 7 * 24 * 3600 * 1000; hist = hist.filter((h) => Date.parse(h.t) >= cutoff);
  fs.writeFileSync(hf, JSON.stringify(hist));
  const peak72 = hist.filter((h) => Date.parse(h.t) >= Date.now() - 72 * 3600 * 1000).reduce((m, h) => Math.max(m, h.players), 0);
  summary.totals.peak72 = peak72; fs.writeFileSync(path.join(OUT, 'summary.json'), JSON.stringify(summary));

  console.log(`listed ${all.length}, online ${live.length}, players ${players}, enriched ${enriched}/${enrichList.length}, peak72 ${peak72}, ${((Date.now() - t0) / 1000).toFixed(1)}s`);
}

if (import.meta.url === `file:///${process.argv[1].replace(/\\/g, '/')}` || process.argv[1].endsWith('collect.mjs')) main().catch((e) => { console.error(e); process.exit(1); });
