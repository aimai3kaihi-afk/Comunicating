// 依存パッケージなしのシンプルなチャット＋共有メモ帳サーバー
// 起動: node server.js  → http://localhost:3000
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const FILE = path.join(DATA_DIR, 'rooms.json');
const INDEX = path.join(__dirname, 'index.html');

const MAX_MESSAGES = 500;      // 部屋ごとに保持する発言数
const MAX_TEXT = 1000;         // 1発言の最大文字数
const MAX_NAME = 20;
const MAX_MEMO = 20000;
const ROOM_TTL_DAYS = 30;      // この日数更新がない部屋は削除
const RATE_LIMIT = 30;         // 1分あたりの書き込み回数(IPごと)

fs.mkdirSync(DATA_DIR, { recursive: true });

let rooms = {};
try { rooms = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch { rooms = {}; }

// ---- 保存(まとめて書く) ----
let dirty = false;
function save() { dirty = true; }
setInterval(() => {
  if (!dirty) return;
  dirty = false;
  const tmp = FILE + '.tmp';
  fs.writeFile(tmp, JSON.stringify(rooms), (e) => {
    if (!e) fs.rename(tmp, FILE, () => {});
  });
}, 2000);

// ---- 古い部屋の掃除 ----
function purge() {
  const limit = Date.now() - ROOM_TTL_DAYS * 86400000;
  for (const k of Object.keys(rooms)) {
    if ((rooms[k].updated || 0) < limit) { delete rooms[k]; save(); }
  }
}
purge();
setInterval(purge, 3600000);

// ---- 部屋 ----
const ROOM_RE = /^[^\/\\\u0000-\u001f]{1,40}$/;
function getRoom(name) {
  if (!Object.prototype.hasOwnProperty.call(rooms, name)) {
    rooms[name] = { messages: [], seq: 0, memo: '', updated: Date.now() };
  }
  return rooms[name];
}

// ---- リアルタイム配信(SSE) ----
const subs = new Map(); // room名 -> Set(res)
function broadcast(name, event, data) {
  const set = subs.get(name);
  if (!set) return;
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of set) res.write(payload);
}
setInterval(() => {
  for (const set of subs.values()) for (const res of set) res.write(': ping\n\n');
}, 25000);

// ---- 簡易レート制限 ----
const hits = new Map();
function limited(ip) {
  const now = Date.now();
  const arr = (hits.get(ip) || []).filter((t) => now - t < 60000);
  arr.push(now);
  hits.set(ip, arr);
  return arr.length > RATE_LIMIT;
}
setInterval(() => {
  const now = Date.now();
  for (const [ip, arr] of hits) if (!arr.some((t) => now - t < 60000)) hits.delete(ip);
}, 60000);

function readJson(req, limit = 64 * 1024) {
  return new Promise((resolve, reject) => {
    let size = 0, chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) { reject(new Error('too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); }
      catch (e) { reject(e); }
    });
    req.on('error', reject);
  });
}

function send(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(JSON.stringify(obj));
}

const clean = (s, max) =>
  String(s ?? '').replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f]/g, '').slice(0, max);

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const p = url.pathname;
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();

  try {
    // 画面
    if (req.method === 'GET' && (p === '/' || p.startsWith('/r/'))) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      return fs.createReadStream(INDEX).pipe(res);
    }
    if (p === '/healthz') return send(res, 200, { ok: true });

    // API
    const m = p.match(/^\/api\/r\/([^\/]+)(?:\/(messages|memo|events))?$/);
    if (!m) return send(res, 404, { error: 'not found' });

    let name;
    try { name = decodeURIComponent(m[1]); } catch { return send(res, 400, { error: 'bad room' }); }
    if (!ROOM_RE.test(name)) return send(res, 400, { error: '部屋名が不正です' });
    const sub = m[2];

    if (req.method === 'GET' && !sub) {
      const r = getRoom(name);
      return send(res, 200, { messages: r.messages, memo: r.memo });
    }

    if (req.method === 'GET' && sub === 'events') {
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache, no-transform',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no',
      });
      res.write('retry: 2000\n\n');
      if (!subs.has(name)) subs.set(name, new Set());
      subs.get(name).add(res);
      req.on('close', () => {
        const set = subs.get(name);
        if (set) { set.delete(res); if (!set.size) subs.delete(name); }
      });
      return;
    }

    if (req.method === 'POST' && sub === 'messages') {
      if (limited(ip)) return send(res, 429, { error: '送信が多すぎます。少し待ってください' });
      const body = await readJson(req);
      const text = clean(body.text, MAX_TEXT).trim();
      if (!text) return send(res, 400, { error: '空のメッセージです' });
      const r = getRoom(name);
      const msg = {
        id: ++r.seq,
        name: clean(body.name, MAX_NAME).trim() || '名無しさん',
        text,
        cid: clean(body.cid, 40),
        at: Date.now(),
      };
      r.messages.push(msg);
      if (r.messages.length > MAX_MESSAGES) r.messages.splice(0, r.messages.length - MAX_MESSAGES);
      r.updated = Date.now();
      save();
      broadcast(name, 'message', msg);
      return send(res, 200, msg);
    }

    if (req.method === 'PUT' && sub === 'memo') {
      if (limited(ip)) return send(res, 429, { error: '送信が多すぎます' });
      const body = await readJson(req, 128 * 1024);
      const r = getRoom(name);
      r.memo = clean(body.text, MAX_MEMO).replace(/[\u0000-\u0008]/g, '');
      r.updated = Date.now();
      save();
      broadcast(name, 'memo', { text: r.memo, cid: clean(body.cid, 40) });
      return send(res, 200, { ok: true });
    }

    return send(res, 405, { error: 'method not allowed' });
  } catch (e) {
    if (!res.headersSent) send(res, 400, { error: 'bad request' });
  }
});

server.listen(PORT, () => console.log(`起動しました: http://localhost:${PORT}`));
