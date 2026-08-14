// MoonCraft multiplayer server — Cloudflare Worker + Durable Object
// 방(room)마다 하나의 WorldDO 인스턴스가 만들어져 월드 시드/모드/블록 편집을 저장하고
// 접속자 간 위치·블록·채팅·몹 동기화 메시지를 중계한다.
// 몹(좀비)은 "호스트"(가장 먼저 접속한 플레이어)의 브라우저가 시뮬레이션하고
// 서버는 그 결과를 다른 플레이어에게 중계만 한다.

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

const MAX_MSG = 8192;
const WORLD_H = 64;

export class WorldDO {
  constructor(state) {
    this.state = state;
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

  currentHost() {
    let best = null, bestT = Infinity;
    for (const s of this.state.getWebSockets()) {
      const a = safeAttach(s);
      if (a && a.id && a.joinT < bestT) { bestT = a.joinT; best = a.id; }
    }
    return best;
  }

  async webSocketMessage(ws, raw) {
    if (typeof raw !== 'string' || raw.length > MAX_MSG) return;
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'join') {
      let seed = await this.state.storage.get('seed');
      let mode = await this.state.storage.get('mode');
      if (seed == null) {
        seed = (Math.random() * 0x7fffffff) | 0;
        mode = m.mode === 's' ? 's' : 'c';
        await this.state.storage.put('seed', seed);
        await this.state.storage.put('mode', mode);
        await this.state.storage.put('t0', Date.now());
      }
      if (mode == null) mode = 'c'; // 업데이트 전에 만들어진 월드는 크리에이티브
      const t0 = (await this.state.storage.get('t0')) || Date.now();
      const id = crypto.randomUUID().slice(0, 8);
      const name = String(m.name || '플레이어').replace(/[\r\n]/g, ' ').slice(0, 16).trim() || '플레이어';
      const uid = (typeof m.uid === 'string' && /^[\w-]{6,40}$/.test(m.uid)) ? m.uid : null;
      ws.serializeAttachment({ id, name, uid, joinT: Date.now(), lastChat: 0 });
      const pdata = uid ? (await this.state.storage.get('p:' + uid)) || null : null;

      const stored = await this.state.storage.list({ prefix: 'e:' });
      const edits = {};
      for (const v of stored.values()) Object.assign(edits, v);

      const players = [];
      for (const s of this.state.getWebSockets()) {
        if (s === ws) continue;
        const a = safeAttach(s);
        if (a && a.id) players.push({ id: a.id, name: a.name, p: a.p || null });
      }

      ws.send(JSON.stringify({
        t: 'init', id, name, seed, mode, t0, now: Date.now(),
        host: this.currentHost(), edits, players, pdata,
      }));
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
    } else if (m.t === 'mobs') {
      // 호스트가 보내는 몹 상태 — 그대로 중계
      if (!Array.isArray(m.l) || m.l.length > 16) return;
      this.broadcast({ t: 'mobs', l: m.l }, ws);
    } else if (m.t === 'mobhit') {
      // 플레이어의 몹 공격 — 호스트가 받아서 처리
      this.broadcast({ t: 'mobhit', i: m.i | 0, d: Math.min(10, Math.max(0, +m.d || 0)), kx: +m.kx || 0, kz: +m.kz || 0 }, ws);
    } else if (m.t === 'summon') {
      // 달의 제단 보스 소환 — 호스트가 처리
      this.broadcast({ t: 'summon', x: +m.x || 0, y: +m.y || 0, z: +m.z || 0, by: a.name }, ws);
    } else if (m.t === 'slam') {
      // 보스 내려찍기 충격파
      this.broadcast({ t: 'slam', x: +m.x || 0, y: +m.y || 0, z: +m.z || 0 }, ws);
    } else if (m.t === 'sleep') {
      // 수면 상태 알림 (호스트가 전원 취침 판정)
      this.broadcast({ t: 'sleep', id: a.id, on: !!m.on }, ws);
    } else if (m.t === 'timeset') {
      // 호스트의 아침 설정 — 저장 후 전원에게 방송
      const t0 = +m.t0;
      if (!Number.isFinite(t0)) return;
      await this.state.storage.put('t0', t0);
      this.broadcast({ t: 't0', t0 }, null);
    } else if (m.t === 'boom') {
      // 크리퍼 폭발 연출/데미지 중계
      this.broadcast({ t: 'boom', x: +m.x || 0, y: +m.y || 0, z: +m.z || 0 }, ws);
    } else if (m.t === 'save') {
      // 플레이어 데이터 저장 (인벤토리/체력/위치)
      if (a.uid && m.d && typeof m.d === 'object') {
        await this.state.storage.put('p:' + a.uid, m.d);
      }
    } else if (m.t === 'copen') {
      // 상자 열기 — 내용물 전송 (fresh = 한 번도 열린 적 없음 → 클라이언트가 보물 생성)
      const key = 'chest:' + (m.x | 0) + ',' + (m.y | 0) + ',' + (m.z | 0);
      const stored = await this.state.storage.get(key);
      ws.send(JSON.stringify({ t: 'chest', x: m.x | 0, y: m.y | 0, z: m.z | 0, s: stored || Array(27).fill(null), fresh: !stored }));
    } else if (m.t === 'cset') {
      // 상자 내용 갱신 — 저장 + 열어둔 다른 사람에게 방송
      if (!Array.isArray(m.s) || m.s.length > 27) return;
      const s = m.s.slice(0, 27).map(v => {
        if (!v || typeof v !== 'object') return null;
        const id = v.id | 0, n = v.n | 0;
        if (id < 1 || id > 130 || n < 1 || n > 64) return null;
        const out = { id, n };
        if (Number.isFinite(v.d)) out.d = v.d | 0;
        if (Number.isFinite(v.ench)) out.ench = Math.min(3, v.ench | 0);
        return out;
      });
      const key = 'chest:' + (m.x | 0) + ',' + (m.y | 0) + ',' + (m.z | 0);
      await this.state.storage.put(key, s);
      this.broadcast({ t: 'chest', x: m.x | 0, y: m.y | 0, z: m.z | 0, s }, ws);
    } else if (m.t === 'cbreak') {
      // 상자 파괴 — 내용물을 부순 사람에게 돌려줌
      const key = 'chest:' + (m.x | 0) + ',' + (m.y | 0) + ',' + (m.z | 0);
      const s = await this.state.storage.get(key);
      await this.state.storage.delete(key);
      const items = [];
      if (Array.isArray(s)) for (const v of s) if (v && v.id) items.push([v.id, v.n || 1]);
      ws.send(JSON.stringify({ t: 'spill', x: m.x | 0, y: m.y | 0, z: m.z | 0, items }));
    }
  }

  webSocketClose(ws) {
    const a = safeAttach(ws);
    if (a && a.id) {
      this.broadcast({ t: 'pleave', id: a.id }, ws);
      const h = this.currentHostExcept(ws);
      if (h) this.broadcast({ t: 'host', id: h }, ws);
    }
  }

  currentHostExcept(except) {
    let best = null, bestT = Infinity;
    for (const s of this.state.getWebSockets()) {
      if (s === except) continue;
      const a = safeAttach(s);
      if (a && a.id && a.joinT < bestT) { bestT = a.joinT; best = a.id; }
    }
    return best;
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
