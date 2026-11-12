import * as http from "http";
import * as path from "path";
import * as zmq from "zeromq";
import * as protobuf from "protobufjs";

const root = protobuf.loadSync(path.join(__dirname, "chat.proto"));
const Publication = root.lookupType("chat.Publication");
const CoordinatorAnnouncement = root.lookupType("chat.CoordinatorAnnouncement");
const ReferenceRequest = root.lookupType("chat.ReferenceRequest");
const ReferenceResponse = root.lookupType("chat.ReferenceResponse");

interface ServerInfo {
  name: string;
  rank: number;
  endpoint: string;
}

interface MessageItem {
  id: string;
  channel: string;
  user: string;
  content: string;
  timestamp: number;
  clock: number;
  lang: "C++" | "Python" | "Java" | "Outro";
}

let coordinator = { name: "-", rank: 0 };
let serversList: ServerInfo[] = [];
const messages: MessageItem[] = [];
const channelStats: Record<string, number> = {};
const langStats = { "C++": 0, "Python": 0, "Java": 0, "Outro": 0 };
let totalMessageCount = 0;
let maxClock = 0;

const clients = new Set<http.ServerResponse>();

function detectLanguage(user: string, channel: string): "C++" | "Python" | "Java" | "Outro" {
  const s = `${user} ${channel}`.toLowerCase();
  if (s.includes("cpp")) return "C++";
  if (s.includes("py")) return "Python";
  if (s.includes("java")) return "Java";
  return "Outro";
}

function broadcast(type: string, data: any) {
  const payload = `data: ${JSON.stringify({ type, ...data })}\n\n`;
  for (const client of clients) {
    try {
      client.write(payload);
    } catch {
      clients.delete(client);
    }
  }
}

const html = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <title>Sistema Distribuído</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #ffffff;
      color: #111111;
      padding: 16px;
      font-size: 13px;
      line-height: 1.4;
    }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    
    header {
      border-bottom: 1px solid #111;
      padding-bottom: 8px;
      margin-bottom: 16px;
      display: flex;
      gap: 24px;
      flex-wrap: wrap;
    }
    .header-item { font-size: 12px; }
    .header-item strong { font-weight: 600; }

    .layout {
      display: grid;
      grid-template-columns: 200px 1fr;
      gap: 24px;
    }

    aside { border-right: 1px solid #eee; padding-right: 16px; }
    h3 { font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; color: #666; margin-bottom: 8px; margin-top: 16px; }
    aside h3:first-child { margin-top: 0; }

    .nav-list { list-style: none; display: flex; flex-direction: column; gap: 2px; }
    .nav-item {
      display: flex;
      justify-content: space-between;
      padding: 4px 6px;
      cursor: pointer;
      border-radius: 2px;
      font-size: 12px;
      color: #333;
    }
    .nav-item:hover { background: #f5f5f5; color: #000; }
    .nav-item.active { background: #111; color: #fff; font-weight: 600; }

    section { margin-bottom: 24px; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th { text-align: left; padding: 6px 8px; border-bottom: 1px solid #111; font-size: 11px; text-transform: uppercase; color: #444; }
    td { padding: 6px 8px; border-bottom: 1px solid #eee; }
    tr:hover td { background: #fafafa; }
  </style>
</head>
<body>
  <header>
    <div class="header-item">LÍDER: <strong class="mono" id="coord-name">-</strong> <span class="mono" id="coord-rank"></span></div>
    <div class="header-item">SERVIDORES: <strong class="mono" id="total-servers">0</strong></div>
    <div class="header-item">MENSAGENS: <strong class="mono" id="total-msgs">0</strong></div>
    <div class="header-item">LAMPORT: <strong class="mono" id="max-clock">0</strong></div>
  </header>

  <div class="layout">
    <aside>
      <h3>Linguagem</h3>
      <ul class="nav-list" id="lang-list">
        <li class="nav-item active" data-lang="ALL" onclick="setLang('ALL')"><span>Todas</span><span class="mono" id="badge-all">0</span></li>
        <li class="nav-item" data-lang="C++" onclick="setLang('C++')"><span>C++</span><span class="mono" id="badge-cpp">0</span></li>
        <li class="nav-item" data-lang="Python" onclick="setLang('Python')"><span>Python</span><span class="mono" id="badge-py">0</span></li>
        <li class="nav-item" data-lang="Java" onclick="setLang('Java')"><span>Java</span><span class="mono" id="badge-java">0</span></li>
      </ul>

      <h3>Canais</h3>
      <ul class="nav-list" id="channel-list" style="max-height: 400px; overflow-y: auto;">
        <li class="nav-item active" data-channel="ALL" onclick="setChannel('ALL')"><span>Todos</span></li>
      </ul>
    </aside>

    <main>
      <section>
        <h3>Servidores</h3>
        <table>
          <thead>
            <tr>
              <th style="width: 60px;">Rank</th>
              <th>Nome</th>
              <th>Endpoint</th>
              <th style="width: 100px;">Função</th>
            </tr>
          </thead>
          <tbody id="servers-table"></tbody>
        </table>
      </section>

      <section>
        <h3>Mensagens</h3>
        <table>
          <thead>
            <tr>
              <th style="width: 80px;">Hora</th>
              <th style="width: 60px;">Clock</th>
              <th style="width: 70px;">Ling</th>
              <th style="width: 200px;">Canal</th>
              <th style="width: 180px;">Usuário</th>
              <th>Mensagem</th>
            </tr>
          </thead>
          <tbody id="messages-table"></tbody>
        </table>
      </section>
    </main>
  </div>

  <script>
    let selectedLang = "ALL";
    let selectedChannel = "ALL";
    let allMessages = [];
    let currentLeader = "";

    function setLang(lang) {
      selectedLang = lang;
      document.querySelectorAll("#lang-list .nav-item").forEach(el => {
        el.classList.toggle("active", el.getAttribute("data-lang") === lang);
      });
      render();
    }

    function setChannel(ch) {
      selectedChannel = ch;
      document.querySelectorAll("#channel-list .nav-item").forEach(el => {
        el.classList.toggle("active", el.getAttribute("data-channel") === ch);
      });
      render();
    }

    function render() {
      const tbody = document.getElementById("messages-table");
      tbody.innerHTML = "";

      const filtered = allMessages.filter(m => {
        const langMatch = selectedLang === "ALL" || m.lang === selectedLang;
        const chanMatch = selectedChannel === "ALL" || m.channel === selectedChannel;
        return langMatch && chanMatch;
      });

      filtered.slice(0, 100).forEach(m => {
        const tr = document.createElement("tr");
        const time = new Date(m.timestamp).toLocaleTimeString();
        tr.innerHTML = \`
          <td class="mono">\${time}</td>
          <td class="mono">\${m.clock}</td>
          <td>\${m.lang}</td>
          <td>\${m.channel}</td>
          <td class="mono">\${m.user}</td>
          <td>\${m.content}</td>
        \`;
        tbody.appendChild(tr);
      });
    }

    function updateChannels(channels) {
      const list = document.getElementById("channel-list");
      const active = selectedChannel;
      list.innerHTML = \`
        <li class="nav-item \${active === "ALL" ? "active" : ""}" data-channel="ALL" onclick="setChannel('ALL')">
          <span>Todos</span>
        </li>
      \`;

      Object.keys(channels).sort().forEach(ch => {
        const li = document.createElement("li");
        li.className = "nav-item " + (active === ch ? "active" : "");
        li.setAttribute("data-channel", ch);
        li.onclick = () => setChannel(ch);
        li.innerHTML = \`<span>\${ch}</span><span class="mono">\${channels[ch]}</span>\`;
        list.appendChild(li);
      });
    }

    function updateServers(servers, leader) {
      const tbody = document.getElementById("servers-table");
      if (!servers) return;
      tbody.innerHTML = "";

      servers.sort((a, b) => b.rank - a.rank).forEach(s => {
        const isLeader = s.name === leader;
        const tr = document.createElement("tr");
        tr.innerHTML = \`
          <td class="mono">#\${s.rank}</td>
          <td class="mono">\${s.name}</td>
          <td class="mono">\${s.endpoint}</td>
          <td>\${isLeader ? "LÍDER" : "RÉPLICA"}</td>
        \`;
        tbody.appendChild(tr);
      });
      document.getElementById("total-servers").innerText = servers.length;
    }

    const es = new EventSource("/events");
    es.onmessage = (e) => {
      const data = JSON.parse(e.data);

      if (data.type === "init") {
        currentLeader = data.coordinator.name;
        document.getElementById("coord-name").innerText = data.coordinator.name || "-";
        document.getElementById("coord-rank").innerText = data.coordinator.rank ? ("(#" + data.coordinator.rank + ")") : "";
        document.getElementById("max-clock").innerText = data.stats.maxClock;
        document.getElementById("total-msgs").innerText = data.stats.totalMessages;
        document.getElementById("badge-all").innerText = data.stats.totalMessages;
        document.getElementById("badge-cpp").innerText = data.stats.langStats["C++"] || 0;
        document.getElementById("badge-py").innerText = data.stats.langStats["Python"] || 0;
        document.getElementById("badge-java").innerText = data.stats.langStats["Java"] || 0;

        allMessages = data.messages || [];
        render();
        updateChannels(data.channels || {});
        updateServers(data.servers, currentLeader);
      } else if (data.type === "message") {
        allMessages.unshift(data.message);
        if (allMessages.length > 500) allMessages.pop();

        document.getElementById("total-msgs").innerText = data.stats.totalMessages;
        document.getElementById("max-clock").innerText = data.stats.maxClock;
        document.getElementById("badge-all").innerText = data.stats.totalMessages;
        document.getElementById("badge-cpp").innerText = data.stats.langStats["C++"] || 0;
        document.getElementById("badge-py").innerText = data.stats.langStats["Python"] || 0;
        document.getElementById("badge-java").innerText = data.stats.langStats["Java"] || 0;
        updateChannels(data.channels);
        render();
      } else if (data.type === "servers") {
        updateServers(data.servers, currentLeader);
      } else if (data.type === "coordinator") {
        currentLeader = data.coordinator.name;
        document.getElementById("coord-name").innerText = data.coordinator.name;
        document.getElementById("coord-rank").innerText = "(#" + data.coordinator.rank + ")";
        updateServers(data.servers, currentLeader);
      }
    };
  </script>
</body>
</html>`;

const server = http.createServer((req, res) => {
  if (req.url === "/events") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive"
    });
    clients.add(res);

    const initData = {
      type: "init",
      coordinator,
      servers: serversList,
      messages: messages.slice(0, 100),
      channels: channelStats,
      stats: {
        totalMessages: totalMessageCount,
        maxClock,
        langStats
      }
    };
    res.write(`data: ${JSON.stringify(initData)}\n\n`);

    req.on("close", () => clients.delete(res));
    return;
  }

  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(html);
});

server.listen(8080, "0.0.0.0", () => {
  console.log("[INTERFACE] Servidor HTTP rodando na porta 8080");
});

async function runSubscriber() {
  const sock = new zmq.Subscriber();
  const proxyUri = process.env.PROXY_URI || "tcp://proxy:5558";
  sock.connect(proxyUri);
  sock.subscribe("");
  sock.subscribe("servers");
  console.log(`[INTERFACE] Inscrito no proxy Pub/Sub: ${proxyUri}`);

  for await (const [topicBuf, dataBuf] of sock) {
    const topic = topicBuf.toString();
    try {
      if (topic === "servers") {
        const ann = CoordinatorAnnouncement.decode(dataBuf) as any;
        coordinator = {
          name: ann.coordinator,
          rank: Number(ann.rank)
        };
        maxClock = Math.max(maxClock, Number(ann.clock || 0));
        broadcast("coordinator", { coordinator, servers: serversList });
        continue;
      }

      if (topic === "replication") {
        continue;
      }

      const msg = Publication.decode(dataBuf) as any;
      const user = msg.user || "desconhecido";
      const channel = msg.channel || topic;
      const lang = detectLanguage(user, channel);
      const clock = msg.clock ? Number(msg.clock) : 0;
      maxClock = Math.max(maxClock, clock);
      totalMessageCount++;

      const item: MessageItem = {
        id: msg.id || `${Date.now()}`,
        channel,
        user,
        content: msg.content || "",
        timestamp: msg.timestamp ? Number(msg.timestamp) : Date.now(),
        clock,
        lang
      };

      messages.unshift(item);
      if (messages.length > 500) messages.pop();

      channelStats[channel] = (channelStats[channel] || 0) + 1;
      langStats[lang] = (langStats[lang] || 0) + 1;

      broadcast("message", {
        message: item,
        channels: channelStats,
        stats: {
          totalMessages: totalMessageCount,
          maxClock,
          langStats
        }
      });
    } catch {}
  }
}

async function pollReference() {
  const refUri = process.env.REFERENCE_URI || "tcp://reference:5559";
  let reqSocket: zmq.Request | null = null;

  while (true) {
    try {
      if (!reqSocket) {
        reqSocket = new zmq.Request();
        reqSocket.receiveTimeout = 1500;
        reqSocket.sendTimeout = 1500;
        reqSocket.connect(refUri);
      }

      const reqMsg = ReferenceRequest.create({
        id: `${Date.now()}_${Math.floor(Math.random() * 10000)}`,
        timestamp: Date.now(),
        clock: maxClock,
        list: {}
      });

      const buf = ReferenceRequest.encode(reqMsg).finish();
      await reqSocket.send(buf);

      const [replyBuf] = await reqSocket.receive();
      const res = ReferenceResponse.decode(replyBuf) as any;

      if (res.list && res.list.servers) {
        serversList = res.list.servers.map((s: any) => ({
          name: s.name,
          rank: Number(s.rank),
          endpoint: s.endpoint
        }));
        broadcast("servers", { servers: serversList });
      }
    } catch {
      if (reqSocket) {
        try { reqSocket.close(); } catch {}
        reqSocket = null;
      }
    }
    await new Promise((r) => setTimeout(r, 3000));
  }
}

runSubscriber();
pollReference();
