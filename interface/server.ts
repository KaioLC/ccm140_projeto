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

let coordinator = { name: "Aguardando...", rank: 0 };
let serversList: ServerInfo[] = [];
const messages: MessageItem[] = [];
const channelStats: Record<string, number> = {};
const langStats = { "C++": 0, "Python": 0, "Java": 0, "Outro": 0 };
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
  <title>Monitor do Sistema Distribuído</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      background: #f8fafc;
      color: #0f172a;
      padding: 24px;
    }
    .container { max-width: 1400px; margin: 0 auto; }
    
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 12px;
      margin-bottom: 20px;
    }
    .metric-card {
      background: #ffffff;
      border: 1px solid #e2e8f0;
      border-radius: 6px;
      padding: 14px 18px;
    }
    .metric-title { font-size: 11px; font-weight: 600; text-transform: uppercase; color: #64748b; letter-spacing: 0.5px; margin-bottom: 4px; }
    .metric-value { font-size: 18px; font-weight: 700; color: #0f172a; font-family: ui-monospace, monospace; }
    .metric-sub { font-size: 12px; color: #16a34a; font-weight: 600; }

    .main-grid {
      display: grid;
      grid-template-columns: 260px 1fr;
      gap: 16px;
      align-items: start;
    }

    .sidebar { display: flex; flex-direction: column; gap: 16px; }
    .panel {
      background: #ffffff;
      border: 1px solid #e2e8f0;
      border-radius: 6px;
      overflow: hidden;
    }
    .panel-header {
      padding: 10px 14px;
      border-bottom: 1px solid #e2e8f0;
      background: #f8fafc;
      font-size: 12px;
      font-weight: 600;
      color: #334155;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .lang-list, .chan-list { padding: 8px; display: flex; flex-direction: column; gap: 4px; }
    .filter-btn {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 8px 12px;
      border-radius: 4px;
      border: 1px solid transparent;
      background: transparent;
      font-size: 13px;
      font-weight: 500;
      color: #334155;
      cursor: pointer;
      text-align: left;
      width: 100%;
    }
    .filter-btn:hover { background: #f1f5f9; }
    .filter-btn.active {
      background: #0f172a;
      color: #ffffff;
      font-weight: 600;
    }
    .filter-btn .badge {
      font-size: 11px;
      font-family: ui-monospace, monospace;
      padding: 2px 6px;
      border-radius: 4px;
      background: #e2e8f0;
      color: #334155;
    }
    .filter-btn.active .badge { background: #334155; color: #ffffff; }

    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th {
      background: #f8fafc;
      color: #475569;
      font-weight: 600;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.4px;
      padding: 8px 12px;
      border-bottom: 1px solid #e2e8f0;
      text-align: left;
    }
    td {
      padding: 8px 12px;
      border-bottom: 1px solid #f1f5f9;
      color: #0f172a;
    }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #fbfcfe; }

    .tag {
      display: inline-block;
      padding: 2px 7px;
      font-size: 11px;
      font-weight: 600;
      border-radius: 4px;
      font-family: ui-monospace, monospace;
    }
    .tag-cpp { background: #eff6ff; color: #1d4ed8; border: 1px solid #bfdbfe; }
    .tag-py { background: #fefce8; color: #854d0e; border: 1px solid #fef08a; }
    .tag-java { background: #fef2f2; color: #991b1b; border: 1px solid #fecaca; }
    .tag-leader { background: #f0fdf4; color: #166534; border: 1px solid #bbf7d0; font-weight: 700; }
    .tag-replica { background: #f8fafc; color: #475569; border: 1px solid #e2e8f0; }

    .mono { font-family: ui-monospace, monospace; font-size: 12px; color: #475569; }
    .bold { font-weight: 600; }
    .channel-name { font-weight: 600; color: #0f172a; }
  </style>
</head>
<body>
  <div class="container">
    <div class="metrics">
      <div class="metric-card">
        <div class="metric-title">Coordenador (Bullying)</div>
        <div class="metric-value" id="coord-name">Aguardando...</div>
        <div class="metric-sub" id="coord-rank">-</div>
      </div>
      <div class="metric-card">
        <div class="metric-title">Servidores Registrados</div>
        <div class="metric-value" id="total-servers">0</div>
        <div class="mono" style="font-size: 11px; color:#64748b;">Referência tcp://reference:5559</div>
      </div>
      <div class="metric-card">
        <div class="metric-title">Total de Mensagens</div>
        <div class="metric-value" id="total-msgs">0</div>
        <div class="mono" style="font-size: 11px; color:#64748b;">Proxy Pub/Sub 5558</div>
      </div>
      <div class="metric-card">
        <div class="metric-title">Relógio Lógico (Lamport)</div>
        <div class="metric-value" id="max-clock">0</div>
        <div class="mono" style="font-size: 11px; color:#64748b;">Maior clock observado</div>
      </div>
    </div>

    <div class="main-grid">
      <div class="sidebar">
        <div class="panel">
          <div class="panel-header">Linguagem</div>
          <div class="lang-list">
            <button class="filter-btn active" data-lang="ALL" onclick="setLangFilter('ALL')">
              <span>Todas</span><span class="badge" id="badge-all">0</span>
            </button>
            <button class="filter-btn" data-lang="C++" onclick="setLangFilter('C++')">
              <span>C++</span><span class="badge" id="badge-cpp">0</span>
            </button>
            <button class="filter-btn" data-lang="Python" onclick="setLangFilter('Python')">
              <span>Python</span><span class="badge" id="badge-py">0</span>
            </button>
            <button class="filter-btn" data-lang="Java" onclick="setLangFilter('Java')">
              <span>Java</span><span class="badge" id="badge-java">0</span>
            </button>
          </div>
        </div>

        <div class="panel">
          <div class="panel-header">
            <span>Canais</span>
            <span id="channel-count" style="font-size: 10px; color:#64748b;">0</span>
          </div>
          <div class="chan-list" id="channel-list" style="max-height: 380px; overflow-y: auto;">
            <button class="filter-btn active" data-channel="ALL" onclick="setChannelFilter('ALL')">
              <span>Todos os canais</span>
            </button>
          </div>
        </div>
      </div>

      <div style="display: flex; flex-direction: column; gap: 16px;">
        <div class="panel">
          <div class="panel-header">Ranking Atual de Servidores (Referência & Eleição)</div>
          <table>
            <thead>
              <tr>
                <th style="width: 80px;">Rank</th>
                <th>Servidor</th>
                <th>Endpoint</th>
                <th style="width: 140px;">Função</th>
              </tr>
            </thead>
            <tbody id="servers-table">
              <tr><td colspan="4" class="mono" style="text-align: center; color: #94a3b8; padding: 16px;">Consultando serviço de referência...</td></tr>
            </tbody>
          </table>
        </div>

        <div class="panel">
          <div class="panel-header">
            <span>Fluxo de Mensagens</span>
            <span class="mono" id="stream-status" style="color: #16a34a; font-size: 11px;">● Conectado</span>
          </div>
          <table>
            <thead>
              <tr>
                <th style="width: 90px;">Horário</th>
                <th style="width: 70px;">Clock</th>
                <th style="width: 80px;">Ling</th>
                <th style="width: 180px;">Canal</th>
                <th style="width: 160px;">Usuário</th>
                <th>Mensagem</th>
              </tr>
            </thead>
            <tbody id="messages-table"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <script>
    let selectedLang = "ALL";
    let selectedChannel = "ALL";
    let allMessages = [];
    let currentCoordinator = "";

    function setLangFilter(lang) {
      selectedLang = lang;
      document.querySelectorAll(".lang-list .filter-btn").forEach(btn => {
        btn.classList.toggle("active", btn.getAttribute("data-lang") === lang);
      });
      renderMessages();
    }

    function setChannelFilter(channel) {
      selectedChannel = channel;
      document.querySelectorAll("#channel-list .filter-btn").forEach(btn => {
        btn.classList.toggle("active", btn.getAttribute("data-channel") === channel);
      });
      renderMessages();
    }

    function renderMessages() {
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
        let tagClass = "tag-cpp";
        if (m.lang === "Python") tagClass = "tag-py";
        if (m.lang === "Java") tagClass = "tag-java";

        tr.innerHTML = \`
          <td class="mono">\${time}</td>
          <td class="mono">\${m.clock}</td>
          <td><span class="tag \${tagClass}">\${m.lang}</span></td>
          <td class="channel-name">\${m.channel}</td>
          <td class="mono bold">\${m.user}</td>
          <td>\${m.content}</td>
        \`;
        tbody.appendChild(tr);
      });
    }

    function updateChannels(channels) {
      const list = document.getElementById("channel-list");
      const active = selectedChannel;
      list.innerHTML = \`
        <button class="filter-btn \${active === "ALL" ? "active" : ""}" data-channel="ALL" onclick="setChannelFilter('ALL')">
          <span>Todos</span>
        </button>
      \`;

      Object.keys(channels).sort().forEach(ch => {
        const count = channels[ch];
        const btn = document.createElement("button");
        btn.className = "filter-btn " + (active === ch ? "active" : "");
        btn.setAttribute("data-channel", ch);
        btn.onclick = () => setChannelFilter(ch);
        btn.innerHTML = \`<span>\${ch}</span><span class="badge">\${count}</span>\`;
        list.appendChild(btn);
      });
      document.getElementById("channel-count").innerText = Object.keys(channels).length;
    }

    function updateServers(servers, leader) {
      const tbody = document.getElementById("servers-table");
      if (!servers || servers.length === 0) return;
      tbody.innerHTML = "";

      servers.sort((a, b) => b.rank - a.rank).forEach(s => {
        const isLeader = s.name === leader;
        const tr = document.createElement("tr");
        tr.innerHTML = \`
          <td class="mono bold">#\${s.rank}</td>
          <td class="bold">\${s.name}</td>
          <td class="mono">\${s.endpoint}</td>
          <td>\${isLeader ? '<span class="tag tag-leader">COORDENADOR</span>' : '<span class="tag tag-replica">RÉPLICA</span>'}</td>
        \`;
        tbody.appendChild(tr);
      });
      document.getElementById("total-servers").innerText = servers.length;
    }

    const es = new EventSource("/events");
    es.onmessage = (e) => {
      const data = JSON.parse(e.data);

      if (data.type === "init") {
        currentCoordinator = data.coordinator.name;
        document.getElementById("coord-name").innerText = data.coordinator.name || "Aguardando...";
        document.getElementById("coord-rank").innerText = data.coordinator.rank ? ("Rank #" + data.coordinator.rank) : "-";
        document.getElementById("max-clock").innerText = data.stats.maxClock;
        document.getElementById("total-msgs").innerText = data.stats.totalMessages;
        document.getElementById("badge-all").innerText = data.stats.totalMessages;
        document.getElementById("badge-cpp").innerText = data.stats.langStats["C++"] || 0;
        document.getElementById("badge-py").innerText = data.stats.langStats["Python"] || 0;
        document.getElementById("badge-java").innerText = data.stats.langStats["Java"] || 0;

        allMessages = data.messages || [];
        renderMessages();
        updateChannels(data.channels || {});
        updateServers(data.servers, currentCoordinator);
      } else if (data.type === "message") {
        allMessages.unshift(data.message);
        document.getElementById("total-msgs").innerText = data.stats.totalMessages;
        document.getElementById("max-clock").innerText = data.stats.maxClock;
        document.getElementById("badge-all").innerText = data.stats.totalMessages;
        document.getElementById("badge-cpp").innerText = data.stats.langStats["C++"] || 0;
        document.getElementById("badge-py").innerText = data.stats.langStats["Python"] || 0;
        document.getElementById("badge-java").innerText = data.stats.langStats["Java"] || 0;
        updateChannels(data.channels);
        renderMessages();
      } else if (data.type === "servers") {
        updateServers(data.servers, currentCoordinator);
      } else if (data.type === "coordinator") {
        currentCoordinator = data.coordinator.name;
        document.getElementById("coord-name").innerText = data.coordinator.name;
        document.getElementById("coord-rank").innerText = "Rank #" + data.coordinator.rank;
        updateServers(data.servers, currentCoordinator);
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
        totalMessages: messages.length,
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
          totalMessages: messages.length,
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
