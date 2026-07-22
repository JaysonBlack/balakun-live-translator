/**
 * Balakun Live Translator — Relay Server
 * Version: 1.0
 *
 * A blind message forwarder. Pairs two clients in a room and passes
 * encrypted payloads between them. It cannot read anything it carries:
 * clients derive their key from a room code the relay never receives.
 *
 * Stores nothing. Logs nothing but connection counts.
 *
 * Run:  npm install  &&  npm start
 * Env:  PORT (default 8787)
 *
 * MIT Licence — Copyright (c) 2026 Choc-Chilla
 */

import http from 'node:http';
import { WebSocketServer } from 'ws';

const PORT = Number(process.env.PORT) || 8787;
const MAX_MEMBERS = 2;
const HEARTBEAT_MS = 20000;

/** roomId (32 hex chars) -> Set<WebSocket> */
const rooms = new Map();

// ---------------------------------------------------------------- http

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.end('ok');
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain' });
  res.end('not found');
});

const wss = new WebSocketServer({ server });

// ---------------------------------------------------------------- utils

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function roomCount() {
  return rooms.size;
}

function leaveRoom(ws) {
  const id = ws.roomId;
  if (!id) return;

  const room = rooms.get(id);
  ws.roomId = null;
  if (!room) return;

  room.delete(ws);
  for (const peer of room) {
    send(peer, { v: 1, type: 'peer-left' });
  }
  if (room.size === 0) {
    rooms.delete(id);
  }
  console.log(`[relay] leave  — rooms:${roomCount()} clients:${wss.clients.size}`);
}

// ---------------------------------------------------------------- sockets

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.roomId = null;

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // ignore anything that isn't JSON
    }
    if (!msg || msg.v !== 1 || typeof msg.type !== 'string') return;

    // --- join a room -------------------------------------------------
    if (msg.type === 'join') {
      const id = String(msg.roomId || '');

      // roomId is SHA-256(code)[0..16] as hex — never the code itself
      if (!/^[a-f0-9]{32}$/.test(id)) {
        send(ws, { v: 1, type: 'error', code: 'bad-room' });
        return;
      }

      leaveRoom(ws);

      let room = rooms.get(id);
      if (!room) {
        room = new Set();
        rooms.set(id, room);
      }
      if (room.size >= MAX_MEMBERS) {
        send(ws, { v: 1, type: 'room-full' });
        return;
      }

      room.add(ws);
      ws.roomId = id;

      send(ws, { v: 1, type: 'joined', peers: room.size - 1 });
      for (const peer of room) {
        if (peer !== ws) send(peer, { v: 1, type: 'peer-joined' });
      }
      console.log(`[relay] join   — rooms:${roomCount()} clients:${wss.clients.size}`);
      return;
    }

    // --- forward opaque payload to the other member ------------------
    if (msg.type === 'data') {
      const room = ws.roomId ? rooms.get(ws.roomId) : null;
      if (!room) return;
      for (const peer of room) {
        if (peer !== ws) send(peer, msg);
      }
      return;
    }

    // --- keepalive ----------------------------------------------------
    if (msg.type === 'ping') {
      send(ws, { v: 1, type: 'pong' });
    }
  });

  ws.on('close', () => leaveRoom(ws));
  ws.on('error', () => leaveRoom(ws));
});

// ---------------------------------------------------------------- heartbeat

const beat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_MS);

wss.on('close', () => clearInterval(beat));

// ---------------------------------------------------------------- start

server.listen(PORT, () => {
  console.log(`Balakun relay v1.0 listening on port ${PORT}`);
  console.log(`Health check: http://localhost:${PORT}/health`);
});
