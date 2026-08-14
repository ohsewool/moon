// MoonCraft multiplayer server — Cloudflare Worker + Durable Object
// 방(room)마다 하나의 WorldDO 인스턴스가 만들어져 월드 시드/블록 편집을 저장하고
// 접속자 간 위치·블록·채팅 메시지를 중계한다.

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/ws') {
      const room = (url.searchParams.get('room') || 'main').replace(/[^\w가-힣-]/g, '').slice(0, 24) || 'main';
      const id = env.WORLD.idFromName(room);
      return env.WORLD.get(id).fetch(request);
    }
    return env.ASSETS.fetch(request);
  },
};

const MAX_MSG = 4096;
const WORLD_H = 64;

export class WorldDO {
  constructor(state) {
    this.state = state;
    // ping/pong은 DO를 깨우지 않고 자동 응답 (연결 유지용)
    this.state.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
  }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('WebSocket upgrade expected', { status: 426 });
    }
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    if (typeof raw !== 'string' || raw.length > MAX_MSG) return;
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'join') {
      let seed = await this.state.storage.get('seed');
      if (seed == null) {
        seed = (Math.random() * 0x7fffffff) | 0;
        await this.state.storage.put('seed', seed);
        await this.state.storage.put('t0', Date.now());
      }
      const t0 = (await this.state.storage.get('t0')) || Date.now();
      const id = crypto.randomUUID().slice(0, 8);
      const name = String(m.name || '플레이어').replace(/[\r\n]/g, ' ').slice(0, 16).trim() || '플레이어';
      ws.serializeAttachment({ id, name, lastChat: 0 });

      // 저장된 모든 블록 편집 수집 (청크 단위 키)
      const stored = await this.state.storage.list({ prefix: 'e:' });
      const edits = {};
      for (const v of stored.values()) Object.assign(edits, v);

      // 현재 접속 중인 다른 플레이어
      const players = [];
      for (const s of this.state.getWebSockets()) {
        if (s === ws) continue;
        const a = safeAttach(s);
        if (a && a.id) players.push({ id: a.id, name: a.name, p: a.p || null });
      }

      ws.send(JSON.stringify({ t: 'init', id, name, seed, t0, now: Date.now(), edits, players }));
      this.broadcast({ t: 'pjoin', id, name }, ws);
      return;
    }

    const a = safeAttach(ws);
    if (!a || !a.id) return;

    if (m.t === 'm') {
      if (!Array.isArray(m.p) || m.p.length !== 3 || !Array.isArray(m.r)) return;
      a.p = { p: m.p, r: m.r, v: +m.v || 0 };
      ws.serializeAttachment(a);
      this.broadcast({ t: 'm', id: a.id, p: m.p, r: m.r, v: a.p.v }, ws);
    } else if (m.t === 'b') {
      const x = m.x | 0, y = m.y | 0, z = m.z | 0, b = m.b | 0;
      if (m.x !== x || m.y !== y || m.z !== z) return;
      if (Math.abs(x) > 4096 || Math.abs(z) > 4096 || y < 0 || y >= WORLD_H) return;
      if (b < 0 || b > 32) return;
      const key = 'e:' + (x >> 4) + ',' + (z >> 4);
      const chunk = (await this.state.storage.get(key)) || {};
      chunk[x + ',' + y + ',' + z] = b;
      await this.state.storage.put(key, chunk);
      this.broadcast({ t: 'b', x, y, z, b }, ws);
    } else if (m.t === 'c') {
      const now = Date.now();
      if (now - (a.lastChat || 0) < 400) return;
      a.lastChat = now;
      ws.serializeAttachment(a);
      const text = String(m.text || '').replace(/[\r\n]/g, ' ').slice(0, 200).trim();
      if (!text) return;
      this.broadcast({ t: 'c', id: a.id, name: a.name, text }, null);
    }
  }

  webSocketClose(ws) {
    const a = safeAttach(ws);
    if (a && a.id) this.broadcast({ t: 'pleave', id: a.id }, ws);
  }

  webSocketError(ws) {
    this.webSocketClose(ws);
  }

  broadcast(obj, except) {
    const s = JSON.stringify(obj);
    for (const w of this.state.getWebSockets()) {
      if (w === except) continue;
      try { w.send(s); } catch { /* 이미 닫힌 소켓 */ }
    }
  }
}

function safeAttach(ws) {
  try { return ws.deserializeAttachment(); } catch { return null; }
}
