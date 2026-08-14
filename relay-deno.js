/**
 * Balakun Live Translator — Relay Server (Deno Deploy, KV-backed)
 * Version: 2.0-deno-kv
 *
 * THE FIX for the "No partner" bug.
 *
 * The old relay kept rooms in each isolate's private memory. Deno Deploy runs
 * many isolates across regions, so two clients could land on different ones and
 * never meet. This version keeps ALL shared state — who is in which room, and
 * the messages between them — in Deno KV, a single database every isolate reads
 * and writes. Whichever isolates the two clients land on, they now share one
 * source of truth.
 *
 * How messages cross isolates:
 *   - A client's message is written to KV as one or more "chunks".
 *   - Chunking exists because KV caps each value at 64 KiB, and proxy-mode
 *     audio (the keyless side's speech) is far larger than that.
 *   - Every isolate polls KV for messages in rooms it holds a local socket for,
 *     reassembles the chunks, delivers them to that socket, then deletes them.
 *
 * The relay stays BLIND: it only ever stores the already-encrypted envelopes
 * the clients produce. It cannot read the conversation.
 *
 * Requires a Deno KV database provisioned and linked to this app.
 *
 * MIT Licence — Copyright (c) 2026 Choc-Chilla
 */

// ---------------------------------------------------------------- config

const MAX_MEMBERS = 2;
const POLL_MS = 500;          // how often each isolate checks KV for new messages
const MEMBER_TTL_MS = 30_000; // a member key expires if not refreshed
const HEARTBEAT_MS = 10_000;  // how often we refresh our members' keys
const CHUNK_TTL_MS = 45_000;  // undelivered message chunks self-expire
const KEEPALIVE_MS = 25_000;  // app-level ping so sockets don't idle out
const CHUNK_CHARS = 40_000;   // max characters per chunk (well under 64 KiB)

// ---------------------------------------------------------------- identity

const ISOLATE_ID = crypto.randomUUID().slice(0, 8);
const BOOT_TIME = new Date().toISOString();

function regionInfo() {
  const keys = ["DENO_REGION", "DENO_DEPLOYMENT_ID", "DENO_DEPLOY_APP_SLUG"];
  const out = {};
  for (const k of keys) {
    try {
      const v = Deno.env.get(k);
      if (v) out[k] = v;
    } catch { /* ignore */ }
  }
  return out;
}
const REGION = regionInfo();

// ---------------------------------------------------------------- KV

let kv = null;
try {
  kv = await Deno.openKv();
  console.log(`[relay] isolate ${ISOLATE_ID} opened KV OK`);
} catch (e) {
  console.error(`[relay] isolate ${ISOLATE_ID} FAILED to open KV:`, e);
}

console.log(
  `[relay] isolate ${ISOLATE_ID} booted ${BOOT_TIME} — region ${JSON.stringify(REGION)}`,
);

// ---------------------------------------------------------------- local state
// (per isolate — just the sockets physically connected here)

/** roomId -> Map<connId, WebSocket> */
const local = new Map();
/** "roomId|connId" -> boolean : last known "a partner is present" */
const peerState = new Map();
/** "roomId|connId" -> Set<msgId> : messages already delivered to this member */
const delivered = new Map();

// ---------------------------------------------------------------- helpers

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

async function roomMembers(roomId) {
  const ids = new Set();
  if (!kv) return ids;
  for await (const e of kv.list({ prefix: ["member", roomId] })) {
    ids.add(e.key[2]);
  }
  return ids;
}

async function roomMessages(roomId) {
  // msgId -> { sender, n, parts: Map<index, dataString> }
  const groups = new Map();
  if (!kv) return groups;
  for await (const e of kv.list({ prefix: ["chunk", roomId] })) {
    const msgId = e.key[2];
    const i = e.key[3];
    const { n, sender, data } = e.value;
    let g = groups.get(msgId);
    if (!g) {
      g = { sender, n, parts: new Map() };
      groups.set(msgId, g);
    }
    g.parts.set(i, data);
  }
  return groups;
}

async function writeMessage(roomId, sender, raw) {
  if (!kv) return;
  const msgId = `${Date.now().toString().padStart(15, "0")}-${
    crypto.randomUUID().slice(0, 8)
  }`;
  const n = Math.max(1, Math.ceil(raw.length / CHUNK_CHARS));
  for (let i = 0; i < n; i++) {
    const data = raw.slice(i * CHUNK_CHARS, (i + 1) * CHUNK_CHARS);
    await kv.set(["chunk", roomId, msgId, i], { i, n, sender, data }, {
      expireIn: CHUNK_TTL_MS,
    });
  }
}

async function deleteMessage(roomId, msgId, n) {
  if (!kv) return;
  for (let i = 0; i < n; i++) {
    await kv.delete(["chunk", roomId, msgId, i]);
  }
}

// ---------------------------------------------------------------- poll loop

let polling = false;
async function poll() {
  if (polling || !kv) return;
  polling = true;
  try {
    for (const roomId of [...local.keys()]) {
      const members = local.get(roomId);
      if (!members || members.size === 0) continue;

      const [memberIds, groups] = await Promise.all([
        roomMembers(roomId),
        roomMessages(roomId),
      ]);
      const sortedMsgIds = [...groups.keys()].sort();

      for (const [connId, ws] of [...members]) {
        const key = roomId + "|" + connId;

        // presence: is anyone else in the room?
        let peerPresent = false;
        for (const id of memberIds) {
          if (id !== connId) {
            peerPresent = true;
            break;
          }
        }
        const prev = peerState.get(key) ?? false;
        if (peerPresent && !prev) send(ws, { v: 1, type: "peer-joined" });
        else if (!peerPresent && prev) send(ws, { v: 1, type: "peer-left" });
        peerState.set(key, peerPresent);

        // deliver any complete messages from the OTHER party
        let delSet = delivered.get(key);
        if (!delSet) {
          delSet = new Set();
          delivered.set(key, delSet);
        }
        for (const msgId of sortedMsgIds) {
          const g = groups.get(msgId);
          if (!g || g.sender === connId) continue;
          if (g.parts.size !== g.n || delSet.has(msgId)) continue;
          let raw = "";
          let ok = true;
          for (let i = 0; i < g.n; i++) {
            const p = g.parts.get(i);
            if (p === undefined) {
              ok = false;
              break;
            }
            raw += p;
          }
          if (!ok) continue;
          if (ws.readyState === WebSocket.OPEN) ws.send(raw);
          delSet.add(msgId);
        }
      }

      // delete any complete message that a local recipient just received
      for (const msgId of sortedMsgIds) {
        const g = groups.get(msgId);
        if (!g || g.parts.size !== g.n) continue;
        let localRecipient = false;
        for (const connId of members.keys()) {
          if (connId !== g.sender) {
            localRecipient = true;
            break;
          }
        }
        if (localRecipient) await deleteMessage(roomId, msgId, g.n);
      }
    }
    prune();
  } catch (e) {
    console.error(`[relay] isolate ${ISOLATE_ID} poll error:`, e);
  } finally {
    polling = false;
  }
}

function liveKey(key) {
  const [roomId, connId] = key.split("|");
  const room = local.get(roomId);
  return !!(room && room.has(connId));
}

function prune() {
  const cutoff = Date.now() - 60_000;
  for (const [key, set] of delivered) {
    for (const msgId of set) {
      const ts = parseInt(msgId.slice(0, 15), 10);
      if (ts < cutoff) set.delete(msgId);
    }
    if (set.size === 0 && !liveKey(key)) delivered.delete(key);
  }
}

setInterval(poll, POLL_MS);

// keep sockets warm so the platform doesn't drop idle connections
setInterval(() => {
  for (const members of local.values()) {
    for (const ws of members.values()) send(ws, { v: 1, type: "keepalive" });
  }
}, KEEPALIVE_MS);

// refresh our members' presence keys before they expire
setInterval(async () => {
  if (!kv) return;
  for (const [roomId, members] of local) {
    for (const connId of members.keys()) {
      try {
        await kv.set(["member", roomId, connId], { ts: Date.now() }, {
          expireIn: MEMBER_TTL_MS,
        });
      } catch { /* ignore */ }
    }
  }
}, HEARTBEAT_MS);

// ---------------------------------------------------------------- sockets

function leave(ws) {
  const roomId = ws.__roomId;
  const connId = ws.__connId;
  ws.__roomId = null;
  if (!roomId) return;

  const room = local.get(roomId);
  if (room) {
    room.delete(connId);
    if (room.size === 0) local.delete(roomId);
  }
  const key = roomId + "|" + connId;
  peerState.delete(key);
  delivered.delete(key);
  if (kv) kv.delete(["member", roomId, connId]).catch(() => {});

  console.log(
    `[relay] LEAVE isolate=${ISOLATE_ID} room=${roomId.slice(0, 8)} conn=${
      (connId || "").slice(0, 8)
    }`,
  );
}

function handleSocket(ws) {
  ws.__roomId = null;
  ws.__connId = null;

  ws.onmessage = async (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      return;
    }
    if (!msg || msg.v !== 1 || typeof msg.type !== "string") return;

    if (msg.type === "join") {
      const id = String(msg.roomId || "");
      if (!/^[a-f0-9]{32}$/.test(id)) {
        send(ws, { v: 1, type: "error", code: "bad-room" });
        return;
      }
      if (!kv) {
        send(ws, { v: 1, type: "error", code: "no-kv" });
        return;
      }
      leave(ws); // clean up if this socket was already in a room

      const existing = await roomMembers(id);
      if (existing.size >= MAX_MEMBERS) {
        send(ws, { v: 1, type: "room-full" });
        return;
      }

      const connId = crypto.randomUUID();
      ws.__roomId = id;
      ws.__connId = connId;
      let room = local.get(id);
      if (!room) {
        room = new Map();
        local.set(id, room);
      }
      room.set(connId, ws);
      await kv.set(["member", id, connId], { ts: Date.now() }, {
        expireIn: MEMBER_TTL_MS,
      });

      const others = existing.size;
      peerState.set(id + "|" + connId, others > 0);
      send(ws, {
        v: 1,
        type: "joined",
        peers: others,
        isolate: ISOLATE_ID,
        region: REGION,
      });
      console.log(
        `[relay] JOIN  isolate=${ISOLATE_ID} room=${id.slice(0, 8)} conn=${
          connId.slice(0, 8)
        } others=${others}`,
      );
      return;
    }

    if (msg.type === "data") {
      const roomId = ws.__roomId;
      const connId = ws.__connId;
      if (!roomId || !connId) return;
      // ev.data is the raw, already-encrypted envelope — forwarded verbatim
      await writeMessage(roomId, connId, ev.data);
      return;
    }

    if (msg.type === "ping") {
      send(ws, { v: 1, type: "pong" });
      return;
    }
  };

  ws.onclose = () => leave(ws);
  ws.onerror = () => leave(ws);
}

// ---------------------------------------------------------------- http

Deno.serve((req) => {
  const url = new URL(req.url);

  if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
    if (url.pathname === "/whereami") {
      return new Response(
        JSON.stringify({
          isolate: ISOLATE_ID,
          booted: BOOT_TIME,
          kv: !!kv,
          localRooms: local.size,
          region: REGION,
        }, null, 2),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }
    if (url.pathname === "/health" || url.pathname === "/") {
      return new Response(kv ? "ok" : "no-kv", {
        status: kv ? 200 : 503,
        headers: { "content-type": "text/plain" },
      });
    }
    return new Response("not found", { status: 404 });
  }

  const { socket, response } = Deno.upgradeWebSocket(req);
  handleSocket(socket);
  return response;
});

console.log(`Balakun relay (Deno KV) v2.0 ready — isolate ${ISOLATE_ID}`);
