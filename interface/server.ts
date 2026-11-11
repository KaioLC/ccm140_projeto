import * as http from "http";
import * as path from "path";
import * as zmq from "zeromq";
import * as protobuf from "protobufjs";

const root = protobuf.loadSync(path.join(__dirname, "chat.proto"));
const Publication = root.lookupType("chat.Publication");

const clients = new Set<http.ServerResponse>();

const html = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <title>Mensagens do Sistema Distribuído</title>
  <style>
    body { font-family: monospace; margin: 20px; background: #fff; color: #111; }
    h2 { margin-bottom: 4px; }
    #status { color: green; font-size: 13px; margin-bottom: 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; }
    th { background: #eee; }
    tr:nth-child(even) { background: #f9f9f9; }
  </style>
</head>
<body>
  <h2>Visualizador de Mensagens</h2>
  <div id="status">&#9679; Conectado ao proxy:5558 (escutando todos os canais)</div>
  <table>
    <thead>
      <tr>
        <th style="width: 90px;">Horário</th>
        <th style="width: 70px;">Clock</th>
        <th style="width: 220px;">Canal</th>
        <th style="width: 180px;">Usuário</th>
        <th>Mensagem</th>
      </tr>
    </thead>
    <tbody id="messages"></tbody>
  </table>
  <script>
    const tbody = document.getElementById("messages");
    const es = new EventSource("/events");
    es.onmessage = (e) => {
      const data = JSON.parse(e.data);
      const tr = document.createElement("tr");
      const time = new Date(data.timestamp).toLocaleTimeString();
      tr.innerHTML = \`
        <td>\${time}</td>
        <td>\${data.clock}</td>
        <td><b>\${data.channel}</b></td>
        <td>\${data.user}</td>
        <td>\${data.content}</td>
      \`;
      tbody.prepend(tr);
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
  console.log(`[INTERFACE] Inscrito no proxy Pub/Sub: ${proxyUri}`);

  for await (const [topicBuf, dataBuf] of sock) {
    const topic = topicBuf.toString();
    try {
      const msg = Publication.decode(dataBuf) as any;
      const eventData = {
        channel: msg.channel || topic,
        user: msg.user,
        content: msg.content,
        timestamp: msg.timestamp ? Number(msg.timestamp) : Date.now(),
        clock: msg.clock ? Number(msg.clock) : 0
      };

      const payload = `data: ${JSON.stringify(eventData)}\n\n`;
      for (const client of clients) {
        client.write(payload);
      }
    } catch {}
  }
}

runSubscriber();
