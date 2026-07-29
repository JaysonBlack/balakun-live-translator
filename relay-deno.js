/**
 * Balakun Live Translator — Relay Server (Deno Deploy edition)
 * Version: 1.0-deno
 *
 * A blind message forwarder. Pairs two clients in a room and passes
 * encrypted payloads between them. It cannot read anything it carries:
 * clients derive their key from a room code the relay never receives.
 *
 * Stores nothing. Logs nothing but connection counts.
 *
 * Runs on Deno Deploy with no dependencies — Deno upgrades WebSockets
 * natively, so there is no `ws` library to install.
 *
 * Endpoints:
 *   GET /health   -> "ok"
 *   GET /         -> "ok" (so the base URL is easy to sanity-check)
 *   WS  (upgrade) -> relay
 *
 * MIT Licence — Copyright (c) 2026 Choc-Chilla
 */

const MAX_MEMBERS = 2;

/** roomId (32 hex chars) -> Set<WebSocket> */
const rooms = new Map();

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function leaveRoom(ws) {
  const id = ws.__roomId;
  if (!id) return;
  ws.__roomId = null;

  const room = rooms.get(id);
  if (!room) return;

  room.delete(ws);
  for (const peer of room) send(peer, { v: 1, type: "peer-left" });
  if (room.size === 0) rooms.delete(id);

  console.log(`[relay] leave — rooms:${rooms.size}`);
}

function handleSocket(ws) {
  ws.__roomId = null;

  ws.onmessage = (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      return; // ignore non-JSON
    }
    if (!msg || msg.v !== 1 || typeof msg.type !== "string") return;

    // --- join a room -------------------------------------------------
    if (msg.type === "join") {
      const id = String(msg.roomId || "");
      if (!/^[a-f0-9]{32}$/.test(id)) {
        send(ws, { v: 1, type: "error", code: "bad-room" });
        return;
      }

      leaveRoom(ws);

      let room = rooms.get(id);
      if (!room) {
        room = new Set();
        rooms.set(id, room);
      }
      if (room.size >= MAX_MEMBERS) {
        send(ws, { v: 1, type: "room-full" });
        return;
      }

      room.add(ws);
      ws.__roomId = id;

      send(ws, { v: 1, type: "joined", peers: room.size - 1 });
      for (const peer of room) {
        if (peer !== ws) send(peer, { v: 1, type: "peer-joined" });
      }
      console.log(`[relay] join  — rooms:${rooms.size}`);
      return;
    }

    // --- forward opaque payload to the other member ------------------
    if (msg.type === "data") {
      const room = ws.__roomId ? rooms.get(ws.__roomId) : null;
      if (!room) return;
      for (const peer of room) {
        if (peer !== ws) send(peer, msg);
      }
      return;
    }

    // --- keepalive ----------------------------------------------------
    if (msg.type === "ping") {
      send(ws, { v: 1, type: "pong" });
    }
  };

  ws.onclose = () => leaveRoom(ws);
  ws.onerror = () => leaveRoom(ws);
}

Deno.serve((req) => {
  const url = new URL(req.url);

  // Plain HTTP checks
  if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    if (url.pathname === "/health" || url.pathname === "/") {
      return new Response("ok", {
        status: 200,
        headers: { "content-type": "text/plain" },
      });
    }
    return new Response("not found", { status: 404 });
  }

  // WebSocket upgrade
  const { socket, response } = Deno.upgradeWebSocket(req);
  handleSocket(socket);
  return response;
});

console.log("Balakun relay (Deno) ready");
