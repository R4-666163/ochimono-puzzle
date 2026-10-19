// Online rooms for the game outside claude.ai.
// /ws/<room name> opens a WebSocket to one Durable Object per room. Each player sends
// presence patches (their board, falling trio, 技 sent); the room relays everyone's
// presence to everyone, the same model as claude.ai's room capability.
// Everything else is the static game in ./site.
import { DurableObject } from "cloudflare:workers";

const MAX_PEERS = 4;
const MAX_PRESENCE = 1900; // a hibernating socket's attachment holds at most 2 KB

export default {
  async fetch(req, env) {
    const m = new URL(req.url).pathname.match(/^\/ws\/([a-z0-9][a-z0-9_.-]{0,47})$/);
    if (!m) return env.ASSETS.fetch(req);
    if (req.headers.get("Upgrade") !== "websocket") return new Response("expected a websocket", { status: 426 });
    return env.ROOMS.get(env.ROOMS.idFromName(m[1])).fetch(req);
  },
};

export class Room extends DurableObject {
  async fetch() {
    if (this.ctx.getWebSockets().length >= MAX_PEERS) return new Response("room is full", { status: 429 });
    const [client, server] = Object.values(new WebSocketPair());
    const peer = crypto.randomUUID().slice(0, 12);
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ peer, presence: {} });
    server.send(JSON.stringify({ t: "hello", peer }));
    this.broadcast();
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, msg) {
    if (typeof msg !== "string" || msg.length > 4096) return;
    let d;
    try { d = JSON.parse(msg); } catch { return; }
    if (!d || d.t !== "p" || !d.patch || typeof d.patch !== "object" || Array.isArray(d.patch)) return;
    const a = ws.deserializeAttachment();
    const presence = { ...a.presence };
    for (const [k, v] of Object.entries(d.patch)) {
      if (v === null) delete presence[k];
      else presence[k] = v;
    }
    if (JSON.stringify(presence).length > MAX_PRESENCE) return;
    ws.serializeAttachment({ peer: a.peer, presence });
    this.broadcast();
  }

  async webSocketClose(ws) { this.broadcast(ws); }
  async webSocketError(ws) { this.broadcast(ws); }

  // everyone's current presence to everyone still here
  broadcast(gone) {
    const socks = this.ctx.getWebSockets().filter((w) => w !== gone);
    const msg = JSON.stringify({ t: "peers", peers: socks.map((w) => w.deserializeAttachment()) });
    for (const w of socks) {
      try { w.send(msg); } catch {}
    }
  }
}
