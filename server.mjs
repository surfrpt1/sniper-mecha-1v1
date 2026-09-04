import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Server as SocketServer } from 'socket.io';

const PORT = Number(process.env.PORT) || 3001;
const HOST = '0.0.0.0';
const ROOT = fileURLToPath(new URL('./', import.meta.url));

// ---- Tuned to match client (index.html BOUNDS=18, speed ~5 u/s) ----
const MAX_PLAYERS = 6;
const MAX_HEALTH = 100;
const TICK = 50; // ms, 20Hz
const PLAYER_RADIUS = 1.2;
const HIT_RADIUS = 1.5;
const MAX_SPEED_UPS = 8; // units/sec (client runs ~5, tolerance for lag)
const KILL_LIMIT = 10;
const COUNTDOWN_SECONDS = 5;
const RESPAWN_MS = 2000;
const WORLD_BOUNDS = 18;

// Weapons (must match client WEAPONS)
const WEAPONS = {
  rifle: { dmg: 35, cooldown: 400, speed: 32, life: 2.0 },
  smg: { dmg: 14, cooldown: 130, speed: 28, life: 1.4 },
  sniper: { dmg: 70, cooldown: 950, speed: 55, life: 2.5 },
};
function weaponStats(id) {
  return WEAPONS[id] || WEAPONS.rifle;
}

// Premium skins (must match client SKINS keys)
const SKINS = ['commando', 'glacier', 'inferno', 'viper'];
function validSkin(s) {
  return SKINS.includes(s) ? s : 'commando';
}

// Authoritative obstacles (must match client OBSTACLES for prediction)
const OBSTACLES = [
  { x: 0, z: 0, w: 4, h: 4 },
  { x: -8, z: -6, w: 3, h: 3 },
  { x: 8, z: 6, w: 3, h: 3 },
  { x: -8, z: 7, w: 2.5, h: 2.5 },
  { x: 8, z: -7, w: 2.5, h: 2.5 },
];

const SPAWNS = [
  { x: -12, y: 0 }, { x: 12, y: 0 },
  { x: -12, y: -10 }, { x: 12, y: 10 },
  { x: 0, y: -12 }, { x: 0, y: 12 },
];

const MIME = {
  '.html': 'text/html', '.js': 'application/javascript', '.css': 'text/css',
  '.json': 'application/json', '.png': 'image/png', '.ico': 'image/x-icon',
  '.svg': 'image/svg+xml', '.map': 'application/json',
  '.txt': 'text/plain', '.xml': 'application/xml', '.mjs': 'application/javascript',
};

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://x');
    // Railway / Docker health checks
    if (url.pathname === '/health' || url.pathname === '/api/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ ok: true, rooms: rooms.size }));
    }
    let urlPath = decodeURIComponent(url.pathname);
    if (urlPath === '/') urlPath = '/index.html';
    const filePath = normalize(join(ROOT, urlPath));
    if (!filePath.startsWith(ROOT)) { res.writeHead(403); return res.end('Forbidden'); }
    const data = await readFile(filePath);
    res.writeHead(200, {
      'Content-Type': MIME[extname(filePath)] || 'application/octet-stream',
      'Cache-Control': urlPath === '/index.html' ? 'no-cache' : 'public, max-age=3600',
    });
    res.end(data);
  } catch (e) {
    res.writeHead(404);
    res.end('Not found');
  }
});

const io = new SocketServer(server, { cors: { origin: '*' } });

// room: { clients, scores, playerIndex, state, hp, names, host, gameState, gameDuration, gameStartTime, countdownInterval, bullets }
const rooms = new Map();
const members = new Map(); // socketId -> { room }
const pendingNames = new Map(); // socketId -> name set before joining

function roomOf(id) {
  const m = members.get(id);
  return m ? m.room : null;
}

function playerIndexFor(room) {
  const used = new Set([...room.playerIndex.values()]);
  for (let i = 0; i < MAX_PLAYERS; i++) if (!used.has(i)) return i;
  return 0;
}

function spawnFor(index) {
  return SPAWNS[index % SPAWNS.length] || { x: 0, y: 0 };
}

function serializeScores(room) {
  const out = [];
  for (const [id, kills] of room.scores) {
    out.push({
      id,
      playerIndex: room.playerIndex.get(id),
      kills,
      name: room.names.get(id) || `P${(room.playerIndex.get(id) || 0) + 1}`,
    });
  }
  return out;
}

function emitRoom(roomName, event, payload) {
  const room = rooms.get(roomName);
  if (!room) return;
  for (const id of room.clients) {
    const s = io.sockets.sockets.get(id);
    if (s) s.emit(event, payload);
  }
}

function broadcastRoomPlayers(roomName) {
  const room = rooms.get(roomName);
  if (!room) return;
  const players = [];
  for (const id of room.clients) {
    players.push({
      id,
      playerIndex: room.playerIndex.get(id),
      name: room.names.get(id) || `P${(room.playerIndex.get(id) || 0) + 1}`,
      isHost: id === room.host,
    });
  }
  emitRoom(roomName, 'room-players', {
    players,
    host: room.host,
    gameState: room.gameState,
    gameDuration: room.gameDuration,
    killLimit: KILL_LIMIT,
  });
}

function hpSnapshot(roomName) {
  const room = rooms.get(roomName);
  if (!room) return [];
  const out = [];
  for (const id of room.clients) {
    const hp = room.hp.get(id);
    if (hp) out.push({ id, playerIndex: room.playerIndex.get(id), hp: hp.hp, alive: hp.alive });
  }
  return out;
}

function endMatch(roomName, room, winnerId) {
  if (room.gameState !== 'playing') return;
  room.gameState = 'ended';
  const scores = serializeScores(room);
  scores.sort((a, b) => b.kills - a.kills);
  const rankings = scores.map((s, i) => ({
    rank: i + 1,
    id: s.id,
    name: s.name,
    playerIndex: s.playerIndex,
    kills: s.kills,
    isWinner: i === 0,
  }));
  emitRoom(roomName, 'match-end', {
    winnerId,
    scores,
    rankings,
    gameDuration: room.gameDuration,
  });
  broadcastRoomPlayers(roomName);
}

function checkWin(roomName, room) {
  for (const [id, kills] of room.scores) {
    if (kills >= KILL_LIMIT) {
      endMatch(roomName, room, id);
      return true;
    }
  }
  return false;
}

function startCountdown(roomName, room) {
  if (room.gameState !== 'waiting' && room.gameState !== 'ended') return;
  // reset for fresh match
  room.gameState = 'countdown';
  for (const id of room.clients) {
    const hp = room.hp.get(id);
    if (hp) { hp.hp = MAX_HEALTH; hp.alive = true; hp.lastFire = 0; }
    const idx = room.playerIndex.get(id) ?? 0;
    const s = spawnFor(idx);
    const st = room.state.get(id);
    if (st) { st.x = s.x; st.y = s.y; st.h = 0; }
  }
  room.bullets = [];
  let remaining = COUNTDOWN_SECONDS;
  broadcastRoomPlayers(roomName);
  emitRoom(roomName, 'countdown', { seconds: remaining });
  if (room.countdownInterval) clearInterval(room.countdownInterval);
  room.countdownInterval = setInterval(() => {
    remaining--;
    if (remaining <= 0) {
      clearInterval(room.countdownInterval);
      room.countdownInterval = null;
      room.gameState = 'playing';
      room.gameStartTime = Date.now();
      emitRoom(roomName, 'game-start', {
        gameDuration: room.gameDuration,
        killLimit: KILL_LIMIT,
        startTime: room.gameStartTime,
        obstacles: OBSTACLES,
      });
      broadcastRoomPlayers(roomName);
    } else {
      emitRoom(roomName, 'countdown', { seconds: remaining });
    }
  }, 1000);
}

function cleanupRoomTimers(room) {
  if (room.countdownInterval) { clearInterval(room.countdownInterval); room.countdownInterval = null; }
}

function pointInObstacle(x, y) {
  for (const o of OBSTACLES) {
    if (Math.abs(x - o.x) < o.w / 2 + PLAYER_RADIUS * 0.5 &&
        Math.abs(y - o.z) < o.h / 2 + PLAYER_RADIUS * 0.5) return true;
  }
  return false;
}

function bulletHitsObstacle(x, y) {
  for (const o of OBSTACLES) {
    if (Math.abs(x - o.x) < o.w / 2 && Math.abs(y - o.z) < o.h / 2) return true;
  }
  return false;
}

io.on('connection', (socket) => {
  console.log(`[+] ${socket.id} connected`);

  socket.on('list-rooms', (cb) => {
    const list = [];
    for (const [name, room] of rooms) {
      list.push({
        name,
        players: room.clients.size,
        count: room.clients.size,
        gameState: room.gameState,
      });
    }
    if (cb) cb(list);
  });

  // Backward compat: join-room(roomName, cb) OR join-room(roomName, playerName, cb)
  socket.on('join-room', (name, nameOrCb, maybeCb) => {
    let playerName = pendingNames.get(socket.id) || null;
    let cb = null;
    if (typeof nameOrCb === 'function') cb = nameOrCb;
    else { playerName = String(nameOrCb || playerName || '').trim().slice(0, 16) || playerName; cb = maybeCb; }

    const clean = String(name || '').trim().slice(0, 24) || 'default';
    if (!rooms.has(clean)) rooms.set(clean, {
      clients: new Set(),
      scores: new Map(),
      playerIndex: new Map(),
      state: new Map(),
      hp: new Map(),
      bullets: [],
      names: new Map(),
      host: null,
      gameState: 'waiting',
      gameDuration: 600,
      gameStartTime: null,
      countdownInterval: null,
    });
    const room = rooms.get(clean);
    if (room.clients.size >= MAX_PLAYERS && !room.clients.has(socket.id)) {
      if (cb) cb({ ok: 0, error: 'Room full' });
      return;
    }
    const prev = roomOf(socket.id);
    if (prev && prev !== clean) leaveRoom(socket);
    if (!room.playerIndex.has(socket.id)) {
      const idx = playerIndexFor(room);
      room.playerIndex.set(socket.id, idx);
      room.scores.set(socket.id, 0);
      room.hp.set(socket.id, { hp: MAX_HEALTH, alive: true, lastFire: 0 });
      const s = spawnFor(idx);
      room.state.set(socket.id, { x: s.x, y: s.y, facingRight: true, angle: 0, skin: 'commando', h: 0 });
      room.names.set(socket.id, playerName || `P${idx + 1}`);
    } else if (playerName) {
      room.names.set(socket.id, playerName);
    }
    room.clients.add(socket.id);
    members.set(socket.id, { room: clean });
    pendingNames.delete(socket.id);
    socket.join(clean);

    if (!room.host || !room.clients.has(room.host)) {
      room.host = socket.id;
    }

    const world = [];
    for (const [id, st] of room.state) {
      if (id !== socket.id && st) {
        world.push({
          id,
          playerIndex: room.playerIndex.get(id),
          name: room.names.get(id) || `P${(room.playerIndex.get(id) || 0) + 1}`,
          ...st,
        });
      }
    }
    socket.emit('match-joined', {
      room: clean,
      playerIndex: room.playerIndex.get(socket.id),
      name: room.names.get(socket.id),
      world,
      scores: serializeScores(room),
      hp: hpSnapshot(clean),
      killLimit: KILL_LIMIT,
      host: room.host,
      gameState: room.gameState,
      gameDuration: room.gameDuration,
      obstacles: OBSTACLES,
      bounds: WORLD_BOUNDS,
    });
    if (cb) cb({ ok: 1, room: clean });
    console.log(`   joined room "${clean}" as P${room.playerIndex.get(socket.id)} (${room.clients.size} players)`);
    broadcastRoomPlayers(clean);
  });

  socket.on('set-name', (name) => {
    const clean = String(name || '').trim().slice(0, 16);
    if (!clean) return;
    const roomName = roomOf(socket.id);
    const room = roomName && rooms.get(roomName);
    if (!room) { pendingNames.set(socket.id, clean); return; }
    room.names.set(socket.id, clean);
    broadcastRoomPlayers(roomName);
  });

  socket.on('set-time-limit', (seconds) => {
    const roomName = roomOf(socket.id);
    const room = roomName && rooms.get(roomName);
    if (!room) return;
    if (socket.id !== room.host) return;
    if (room.gameState !== 'waiting' && room.gameState !== 'ended') return;
    const valid = [120, 300, 600, 900];
    room.gameDuration = valid.includes(seconds) ? seconds : 600;
    broadcastRoomPlayers(roomName);
  });

  socket.on('start-game', () => {
    const roomName = roomOf(socket.id);
    const room = roomName && rooms.get(roomName);
    if (!room) return;
    if (socket.id !== room.host) return;
    if (room.clients.size < 1) return;
    startCountdown(roomName, room);
  });

  socket.on('leave-room', () => leaveRoom(socket));

  socket.on('player-state', (state) => {
    const roomName = roomOf(socket.id);
    const room = roomName && rooms.get(roomName);
    if (!room) return;
    if (room.gameState !== 'playing') return;
    const hp = room.hp.get(socket.id);
    if (!hp || !hp.alive) return;
    const cur = room.state.get(socket.id);
    let x = Number(state.x), y = Number(state.y);
    if (!Number.isFinite(x) || !Number.isFinite(y)) return;
    x = Math.max(-WORLD_BOUNDS, Math.min(WORLD_BOUNDS, x));
    y = Math.max(-WORLD_BOUNDS, Math.min(WORLD_BOUNDS, y));
    if (cur && Number.isFinite(cur.x)) {
      const dx = x - cur.x, dy = y - cur.y;
      const d = Math.hypot(dx, dy);
      const maxStep = MAX_SPEED_UPS * (TICK / 1000) * 2.5;
      if (d > maxStep) {
        x = cur.x + (dx / d) * maxStep;
        y = cur.y + (dy / d) * maxStep;
      }
    }
    // Block walking through obstacles
    if (pointInObstacle(x, y)) {
      if (cur) { x = cur.x; y = cur.y; }
      else return;
    }
    room.state.set(socket.id, {
      x, y,
      facingRight: !!state.facingRight,
      angle: Number(state.angle) || 0,
      skin: validSkin(state.skin),
      h: (() => {
        let h = Number(state.h) || 0;
        h = Math.max(0, Math.min(2.6, h));
        if (cur && Number.isFinite(cur.h)) {
          const dh = h - cur.h;
          if (Math.abs(dh) > 1.0) h = cur.h + Math.sign(dh) * 1.0;
        }
        return h;
      })(),
    });
  });

  socket.on('player-fire', (data) => {
    const roomName = roomOf(socket.id);
    const room = roomName && rooms.get(roomName);
    if (!room) return;
    if (room.gameState !== 'playing') return;
    const hp = room.hp.get(socket.id);
    if (!hp || !hp.alive) return;
    const st = room.state.get(socket.id);
    if (!st) return;
    const now = Date.now();
    const w = weaponStats(data && data.weapon);
    const wid = (data && WEAPONS[data.weapon]) ? data.weapon : 'rifle';
    if (now - hp.lastFire < w.cooldown) return;
    hp.lastFire = now;

    const angle = Number(data.angle) || 0;
    // Muzzle offset 1.5 units forward (matches client visual)
    const sx = st.x + Math.cos(angle) * 1.5;
    const sy = st.y + Math.sin(angle) * 1.5;
    const vx = Math.cos(angle) * w.speed;
    const vy = Math.sin(angle) * w.speed;
    room.bullets.push({ owner: socket.id, x: sx, y: sy, vx, vy, life: w.life, dmg: w.dmg, weapon: wid });

    socket.to(roomName).emit('fx-shot', { id: socket.id, x: sx, y: sy, angle, weapon: wid });
  });

  socket.on('disconnect', () => {
    pendingNames.delete(socket.id);
    leaveRoom(socket);
    console.log(`[-] ${socket.id} disconnected`);
  });
});

function leaveRoom(socket) {
  const roomName = roomOf(socket.id);
  if (!roomName) return;
  const room = rooms.get(roomName);
  socket.leave(roomName);
  members.delete(socket.id);
  if (room) {
    room.clients.delete(socket.id);
    room.playerIndex.delete(socket.id);
    room.scores.delete(socket.id);
    room.state.delete(socket.id);
    room.hp.delete(socket.id);
    room.names.delete(socket.id);
    socket.to(roomName).emit('player-left', socket.id);

    if (room.host === socket.id) {
      room.host = room.clients.values().next().value || null;
    }

    if (room.clients.size === 0) {
      cleanupRoomTimers(room);
      rooms.delete(roomName);
      console.log(`   room "${roomName}" closed (empty)`);
    } else {
      // If countdown with no players left playing, cancel back to waiting
      if (room.gameState === 'countdown' && room.clients.size < 1) {
        cleanupRoomTimers(room);
        room.gameState = 'waiting';
      }
      broadcastRoomPlayers(roomName);
    }
  }
}

// Ballistics simulation @20Hz — single combined broadcast
setInterval(() => {
  for (const [roomName, room] of rooms) {
    if (room.gameState !== 'playing') continue;
    const dt = TICK / 1000;

    if (room.gameStartTime && room.gameDuration) {
      const elapsed = (Date.now() - room.gameStartTime) / 1000;
      if (elapsed >= room.gameDuration) {
        let winnerId = null;
        let maxKills = -1;
        for (const [id, kills] of room.scores) {
          if (kills > maxKills) { maxKills = kills; winnerId = id; }
        }
        endMatch(roomName, room, winnerId);
        continue;
      }
    }

    const newBullets = [];
    for (const b of room.bullets) {
      b.x += b.vx * dt;
      b.y += b.vy * dt;
      b.life -= dt;
      if (b.life <= 0) continue;
      if (Math.abs(b.x) > WORLD_BOUNDS + 4 || Math.abs(b.y) > WORLD_BOUNDS + 4) continue;
      if (bulletHitsObstacle(b.x, b.y)) continue;
      let hit = false;
      for (const id of room.clients) {
        if (id === b.owner) continue;
        const hp = room.hp.get(id);
        if (!hp || !hp.alive) continue;
        const st = room.state.get(id);
        if (!st) continue;
        const dx = b.x - st.x;
        const dy = b.y - st.y;
        if (Math.hypot(dx, dy) < HIT_RADIUS + PLAYER_RADIUS) {
          hp.hp -= (b.dmg || 35);
          // hit feedback to victim + killer
          const victimSock = io.sockets.sockets.get(id);
          if (victimSock) victimSock.emit('fx-hit', { hp: Math.max(0, hp.hp) });
          if (hp.hp <= 0) {
            hp.hp = 0;
            hp.alive = false;
            const killer = room.scores.get(b.owner);
            if (killer !== undefined) room.scores.set(b.owner, killer + 1);
            const killerName = room.names.get(b.owner) || '???';
            const victimName = room.names.get(id) || '???';
            emitRoom(roomName, 'kill-feed', { killer: killerName, victim: victimName, killerId: b.owner, victimId: id });
            const s = spawnFor(room.playerIndex.get(id) ?? 0);
            setTimeout(() => {
              const r = rooms.get(roomName);
              if (!r) return;
              const hp2 = r.hp.get(id);
              if (hp2) { hp2.hp = MAX_HEALTH; hp2.alive = true; }
              const st2 = r.state.get(id);
              if (st2) { st2.x = s.x; st2.y = s.y; st2.h = 0; }
            }, RESPAWN_MS);
          }
          hit = true;
          break;
        }
      }
      if (!hit) newBullets.push(b);
    }
    room.bullets = newBullets;

    const players = [];
    for (const id of room.clients) {
      const st = room.state.get(id);
      if (st) {
        players.push({
          id,
          playerIndex: room.playerIndex.get(id),
          name: room.names.get(id) || `P${(room.playerIndex.get(id) || 0) + 1}`,
          x: st.x, y: st.y,
          angle: st.angle,
          facingRight: st.facingRight,
          skin: validSkin(st.skin),
          h: Number(st.h) || 0,
        });
      }
    }
    emitRoom(roomName, 'world-update', {
      players,
      bullets: room.bullets.map(b => ({ x: b.x, y: b.y, owner: b.owner, weapon: b.weapon || 'rifle' })),
      hp: hpSnapshot(roomName),
      scores: serializeScores(room),
    });

    checkWin(roomName, room);
  }
}, TICK);

server.listen(PORT, HOST, () => {
  console.log(`Sniper Mecha server on http://${HOST}:${PORT} (PORT env ${process.env.PORT || 'unset'})`);
});
