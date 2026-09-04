import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Server as SocketServer } from 'socket.io';

const PORT = process.env.PORT || 3001;
const ROOT = fileURLToPath(new URL('./', import.meta.url));

const MAX_PLAYERS = 6;
const MAX_HEALTH = 100;
const TICK = 50;
const HIT_RADIUS = 22;
const PLAYER_RADIUS = 16;
const MAX_SPEED = 600;
const KILL_LIMIT = 10;
const COUNTDOWN_SECONDS = 5;

const MIME = {
  '.html': 'text/html', '.js': 'application/javascript', '.css': 'text/css',
  '.json': 'application/json', '.png': 'image/png', '.ico': 'image/x-icon',
  '.svg': 'image/svg+xml', '.map': 'application/json',
  '.txt': 'text/plain', '.xml': 'application/xml',
};

const server = createServer(async (req, res) => {
  try {
    let urlPath = decodeURIComponent(new URL(req.url, 'http://x').pathname);
    if (urlPath === '/') urlPath = '/index.html';
    const filePath = normalize(join(ROOT, urlPath));
    if (!filePath.startsWith(ROOT)) { res.writeHead(403); return res.end('Forbidden'); }
    const data = await readFile(filePath);
    res.writeHead(200, { 'Content-Type': MIME[extname(filePath)] || 'application/octet-stream' });
    res.end(data);
  } catch (e) {
    res.writeHead(404);
    res.end('Not found');
  }
});

const io = new SocketServer(server, { cors: { origin: '*' } });

// room: { clients: Set, scores: Map, playerIndex: Map, state: Map, hp: Map, names: Map, host, gameState, gameDuration, gameStartTime, countdownInterval, bullets: [] }
const rooms = new Map();
const members = new Map(); // socketId -> { room }

function roomOf(id) {
  const m = members.get(id);
  return m ? m.room : null;
}

function playerIndexFor(room) {
  const used = new Set([...room.playerIndex.values()]);
  for (let i = 0; i < MAX_PLAYERS; i++) if (!used.has(i)) return i;
  return 0;
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

function broadcastScores(roomName) {
  const room = rooms.get(roomName);
  if (!room) return;
  emitRoom(roomName, 'score-update', { scores: serializeScores(room) });
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

function broadcastHP(roomName) {
  emitRoom(roomName, 'hp-update', { players: hpSnapshot(roomName) });
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
  if (room.gameState !== 'waiting') return;
  room.gameState = 'countdown';
  let remaining = COUNTDOWN_SECONDS;
  broadcastRoomPlayers(roomName);
  emitRoom(roomName, 'countdown', { seconds: remaining });
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

  socket.on('join-room', (name, cb) => {
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
    const prev = roomOf(socket.id);
    if (prev && prev !== clean) leaveRoom(socket);
    if (!room.playerIndex.has(socket.id)) {
      room.playerIndex.set(socket.id, playerIndexFor(room));
      room.scores.set(socket.id, 0);
      room.hp.set(socket.id, { hp: MAX_HEALTH, alive: true, lastFire: 0 });
      room.state.set(socket.id, null);
      room.names.set(socket.id, `P${room.playerIndex.get(socket.id) + 1}`);
    }
    room.clients.add(socket.id);
    members.set(socket.id, { room: clean });
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
    });
    if (cb) cb({ ok: 1, room: clean });
    console.log(`   joined room "${clean}" as P${room.playerIndex.get(socket.id)} (${room.clients.size} players)`);
    broadcastRoomPlayers(clean);
  });

  socket.on('set-name', (name) => {
    const roomName = roomOf(socket.id);
    const room = roomName && rooms.get(roomName);
    if (!room) return;
    const clean = String(name || '').trim().slice(0, 16) || `P${(room.playerIndex.get(socket.id) || 0) + 1}`;
    room.names.set(socket.id, clean);
    broadcastRoomPlayers(roomName);
  });

  socket.on('set-time-limit', (seconds) => {
    const roomName = roomOf(socket.id);
    const room = roomName && rooms.get(roomName);
    if (!room) return;
    if (socket.id !== room.host) return;
    if (room.gameState !== 'waiting') return;
    const valid = [300, 600, 900];
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
    const cur = room.state.get(socket.id);
    let x = Number(state.x) || 0, y = Number(state.y) || 0;
    if (cur && cur.x !== undefined) {
      const dx = x - cur.x, dy = y - cur.y;
      const d = Math.hypot(dx, dy);
      const maxStep = MAX_SPEED * (TICK / 1000) * 2.5;
      if (d > maxStep) {
        x = cur.x + (dx / d) * maxStep;
        y = cur.y + (dy / d) * maxStep;
      }
    }
    room.state.set(socket.id, {
      x, y,
      facingRight: !!state.facingRight,
      angle: Number(state.angle) || 0,
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
    const cooldown = 400; // ms
    if (now - hp.lastFire < cooldown) return;
    hp.lastFire = now;

    const angle = Number(data.angle) || 0;
    const sx = st.x, sy = st.y - 8;
    if (data.x !== undefined && data.y !== undefined) {
      if (Math.hypot(Number(data.x) - sx, Number(data.y) - sy) > 300) return;
    }
    const bulletSpeed = 800;
    const vx = Math.cos(angle) * bulletSpeed;
    const vy = Math.sin(angle) * bulletSpeed;
    room.bullets.push({
      owner: socket.id,
      x: sx, y: sy,
      vx, vy,
      life: 2.0, // seconds
    });

    socket.to(roomName).emit('fx-shot', {
      id: socket.id,
      x: sx, y: sy,
      angle: angle,
    });
  });

  socket.on('disconnect', () => {
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
      broadcastRoomPlayers(roomName);
    }
  }
}

// Ballistics simulation
setInterval(() => {
  for (const [roomName, room] of rooms) {
    if (room.gameState !== 'playing') continue;
    const dt = TICK / 1000;

    // Time limit
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

    // Move bullets and check hits
    const newBullets = [];
    for (const b of room.bullets) {
      b.x += b.vx * dt;
      b.y += b.vy * dt;
      b.life -= dt;
      if (b.life <= 0) continue;
      // Check hit against all players except owner
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
          hp.hp -= 35;
          if (hp.hp <= 0) {
            hp.hp = 0;
            hp.alive = false;
            // Increase killer's score
            const killer = room.scores.get(b.owner);
            if (killer !== undefined) room.scores.set(b.owner, killer + 1);
            // Respawn after 2 seconds
            setTimeout(() => {
              const hp2 = room.hp.get(id);
              if (hp2) { hp2.hp = MAX_HEALTH; hp2.alive = true; }
              broadcastHP(roomName);
              // Also reset position to random spawn
              const st2 = room.state.get(id);
              if (st2) {
                st2.x = (Math.random() - 0.5) * 800;
                st2.y = (Math.random() - 0.5) * 600;
              }
            }, 2000);
          }
          hit = true;
          break;
        }
      }
      if (hit) continue;
      // Boundary check
      if (b.x < -800 || b.x > 800 || b.y < -800 || b.y > 800) continue;
      newBullets.push(b);
    }
    room.bullets = newBullets;

    // Broadcast states to all clients in room
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
        });
      }
    }
    emitRoom(roomName, 'world-update', {
      players,
      bullets: room.bullets.map(b => ({ x: b.x, y: b.y, owner: b.owner })),
      hp: hpSnapshot(roomName),
      scores: serializeScores(room),
    });

    broadcastHP(roomName);
    broadcastScores(roomName);
    checkWin(roomName, room);
  }
}, TICK);

server.listen(PORT, () => {
  console.log(`Sniper Mecha Multiplayer server running on port ${PORT}`);
});