/**
 * Balakun Live Translator — Relay Server (Deno Deploy edition)
 * Version: 1.1-deno  (INSTRUMENTED — diagnostic build)
 *
 * A blind message forwarder. Pairs two clients in a room and passes
 * encrypted payloads between them. It cannot read anything it carries:
 * clients derive their key from a room code the relay never receives.
 *
 * v1.1 adds ONLY diagnostics — the pairing logic is unchanged from v1.0:
 *   - a per-isolate ID (ISOLATE_ID), unique to each running instance
 *   - that ID + roomId printed on every join/leave (read in the Logs tab)
 *   - a /whereami endpoint reporting this isolate's ID and boot time
 *   - the isolate ID included in the "joined" message sent to the client
 *
 * Purpose: prove whether two clients land on the SAME isolate or DIFFERENT
 * isolates. Different isolates = the rooms Map is not shared = the pairing
 * failure we are chasing.
 *
 * MIT Licence — Copyright (c) 2026 Choc-Chilla
 */

const MAX_MEMBERS = 2;

// A random ID for THIS running isolate. If two clients report different
// values, they are being served by different instances that do not share
// the in-memory rooms map below.
const ISOLATE_ID = crypto.randomUUID().slice(0, 8);
const BOOT_TIME = new Date().toISOString();

// Best-effort region/environment probe. The new Deno Deploy does not
// document a region variable, so several are tried; ISOLATE_ID is the
// reliable discriminator regardless.
function regionInfo() {
  const keys = [
    "DENO_REGION",
    "DENO_DEPLOYMENT_ID",
    "DENO_DEPLOY_REVISION_ID",
    "DENO_DEPLOY_APP_SLUG",
  ];
  const out = {};
  for (const k of keys) {
    try {
      const v = Deno.env.get(k);
      if (v) out[k] = v;
    } catch {
      /* env not permitted — ignore */
    }
  }
  return out;
}
const REGION = regionInfo();

console.log(
  `[relay] isolate ${ISOLATE_ID} booted at ${BOOT_TIME} — region info: ${JSON.stringify(REGION)}`,
);

/** roomId (32 hex chars) -> Set<WebSocket>  (LOCAL to this isolate only) */
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

  console.log(
    `[relay] LEAVE  isolate=${ISOLATE_ID} room=${id.slice(0, 8)} localMembers=${room.size} totalRooms=${rooms.size}`,
  );
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

      // The isolate ID rides along in the joined message so the client's
      // debug log can show it too.
      send(ws, {
        v: 1,
        type: "joined",
        peers: room.size - 1,
        isolate: ISOLATE_ID,
        region: REGION,
      });
      for (const peer of room) {
        if (peer !== ws) send(peer, { v: 1, type: "peer-joined" });
      }

      console.log(
        `[relay] JOIN   isolate=${ISOLATE_ID} room=${id.slice(0, 8)} localMembers=${room.size} totalRooms=${rooms.size}`,
      );
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
    if (url.pathname === "/whereami") {
      return new Response(
        JSON.stringify(
          {
            isolate: ISOLATE_ID,
            booted: BOOT_TIME,
            localRooms: rooms.size,
            region: REGION,
          },
          null,
          2,
        ),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }
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

console.log(`Balakun relay (Deno) v1.1 ready — isolate ${ISOLATE_ID}`);
