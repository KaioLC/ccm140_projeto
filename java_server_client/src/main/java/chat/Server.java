package chat;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Server {
    private final String serverName;
    private final String endpoint;
    private final String proxySubUri;
    private volatile String coordinator = "";
    private int rank = 0;
    private long clock = 0;
    private long clockOffset = 0;
    private int reqCount = 0;
    private volatile boolean running = true;

    private final ZContext context;
    private final ZMQ.Socket repSocket;
    private final ZMQ.Socket pubSocket;
    private final ZMQ.Socket refSocket;
    private final Database db;
    private final Random random = new Random();

    public Server(String brokerUri, String proxyPubUri, String proxySubUri,
                  String refUri, String name, String hostId, String dbPath) {
        this.serverName = name;
        this.endpoint = "tcp://" + hostId + ":6000";
        this.proxySubUri = proxySubUri;

        this.context = new ZContext();
        this.repSocket = context.createSocket(SocketType.REP);
        this.repSocket.connect(brokerUri);

        this.pubSocket = context.createSocket(SocketType.PUB);
        this.pubSocket.connect(proxyPubUri);

        this.refSocket = context.createSocket(SocketType.REQ);
        this.refSocket.connect(refUri);

        this.db = new Database(dbPath);
    }

    private long getSystemTime() {
        return System.currentTimeMillis();
    }

    private long getTimestamp() {
        return getSystemTime() + clockOffset;
    }

    private synchronized String genId() {
        return getTimestamp() + "_" + random.nextInt(10000);
    }

    private synchronized long updateClock(long msgClock) {
        clock = Math.max(clock, msgClock);
        return clock;
    }

    private synchronized long incrementClock() {
        return ++clock;
    }

    private void registerInReference() {
        try {
            Chat.ReferenceRequest req = Chat.ReferenceRequest.newBuilder()
                    .setId(genId())
                    .setTimestamp(getTimestamp())
                    .setClock(incrementClock())
                    .setReg(Chat.RegisterRequest.newBuilder()
                            .setName(serverName)
                            .setEndpoint(endpoint)
                            .build())
                    .build();

            refSocket.send(req.toByteArray(), 0);
            byte[] reply = refSocket.recv(0);

            Chat.ReferenceResponse res = Chat.ReferenceResponse.parseFrom(reply);
            updateClock(res.getClock());
            rank = res.getReg().getRank();

            System.out.println("[REF] Registrado: nome=" + serverName
                    + " rank=" + rank + " endpoint=" + endpoint);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Chat.ServerInfo> fetchServerList() {
        List<Chat.ServerInfo> list = new ArrayList<>();
        try {
            Chat.ReferenceRequest req = Chat.ReferenceRequest.newBuilder()
                    .setId(genId())
                    .setTimestamp(getTimestamp())
                    .setClock(incrementClock())
                    .setList(Chat.ServerListRequest.newBuilder().build())
                    .build();

            refSocket.send(req.toByteArray(), 0);
            byte[] reply = refSocket.recv(0);

            Chat.ReferenceResponse res = Chat.ReferenceResponse.parseFrom(reply);
            updateClock(res.getClock());
            list.addAll(res.getList().getServersList());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    private void announceCoordinator() {
        coordinator = serverName;
        Chat.CoordinatorAnnouncement ann = Chat.CoordinatorAnnouncement.newBuilder()
                .setCoordinator(serverName)
                .setRank(rank)
                .setTimestamp(getTimestamp())
                .setClock(incrementClock())
                .build();

        pubSocket.send("servers".getBytes(ZMQ.CHARSET), ZMQ.SNDMORE);
        pubSocket.send(ann.toByteArray(), 0);

        System.out.println("[ELECTION] Eleito coordenador: " + serverName + " (rank " + rank + ")");
    }

    private void startElection() {
        System.out.println("[ELECTION] Iniciando eleicao por bullying...");
        List<Chat.ServerInfo> servers = fetchServerList();
        boolean higherAlive = false;

        for (Chat.ServerInfo s : servers) {
            if (s.getRank() > rank && !s.getName().equals(serverName) && !s.getEndpoint().isEmpty()) {
                try (ZMQ.Socket req = context.createSocket(SocketType.REQ)) {
                    req.setReceiveTimeOut(1000);
                    req.setSendTimeOut(1000);
                    req.connect(s.getEndpoint());

                    Chat.ServerMessage msg = Chat.ServerMessage.newBuilder()
                            .setId(genId())
                            .setTimestamp(getTimestamp())
                            .setClock(incrementClock())
                            .setElection(Chat.ElectionRequest.newBuilder()
                                    .setServer(serverName)
                                    .setRank(rank)
                                    .build())
                            .build();

                    req.send(msg.toByteArray(), 0);
                    byte[] reply = req.recv(0);
                    if (reply != null) {
                        Chat.ServerMessage rep = Chat.ServerMessage.parseFrom(reply);
                        updateClock(rep.getClock());
                        if (rep.hasElectionRes() && rep.getElectionRes().getStatus()) {
                            higherAlive = true;
                            System.out.println("[ELECTION] Servidor superior " + s.getName() + " respondeu OK");
                        }
                    }
                } catch (Exception ignored) {}
            }
        }

        if (!higherAlive) {
            announceCoordinator();
        }
    }

    private void syncClockWithCoordinator() {
        if (serverName.equals(coordinator)) return;

        if (coordinator.isEmpty()) {
            startElection();
            return;
        }

        String coordEndpoint = "";
        List<Chat.ServerInfo> servers = fetchServerList();
        for (Chat.ServerInfo s : servers) {
            if (s.getName().equals(coordinator)) {
                coordEndpoint = s.getEndpoint();
                break;
            }
        }

        if (coordEndpoint.isEmpty()) {
            startElection();
            return;
        }

        try (ZMQ.Socket req = context.createSocket(SocketType.REQ)) {
            req.setReceiveTimeOut(1500);
            req.setSendTimeOut(1500);
            req.connect(coordEndpoint);

            Chat.ServerMessage msg = Chat.ServerMessage.newBuilder()
                    .setId(genId())
                    .setTimestamp(getTimestamp())
                    .setClock(incrementClock())
                    .setClockSync(Chat.ClockSyncRequest.newBuilder()
                            .setServer(serverName)
                            .build())
                    .build();

            req.send(msg.toByteArray(), 0);
            byte[] reply = req.recv(0);
            if (reply != null) {
                Chat.ServerMessage rep = Chat.ServerMessage.parseFrom(reply);
                updateClock(rep.getClock());
                if (rep.hasClockRes()) {
                    long coordTime = rep.getClockRes().getPhysicalTime();
                    long sysNow = getSystemTime();
                    clockOffset = coordTime - sysNow;
                    System.out.println("[BERKELEY] Sincronizado com coordenador " + coordinator
                            + " | Offset: " + clockOffset + "ms | Hora: " + getTimestamp());
                    return;
                }
            }
        } catch (Exception ignored) {}

        System.out.println("[WARN] Coordenador " + coordinator + " nao respondeu ao relogio.");
        startElection();
    }

    private void sendHeartbeat() {
        try {
            Chat.ReferenceRequest req = Chat.ReferenceRequest.newBuilder()
                    .setId(genId())
                    .setTimestamp(getTimestamp())
                    .setClock(incrementClock())
                    .setHeartbeat(Chat.HeartbeatRequest.newBuilder().setName(serverName).build())
                    .build();

            refSocket.send(req.toByteArray(), 0);
            byte[] reply = refSocket.recv(0);
            if (reply != null) {
                Chat.ReferenceResponse res = Chat.ReferenceResponse.parseFrom(reply);
                updateClock(res.getClock());
                System.out.println("[HEARTBEAT] Enviado (msg #" + reqCount + ") | Clock: " + clock);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void replicateEvent(Chat.ReplicationType type, String id, String user,
                                String channel, String content, String name, long ts) {
        Chat.ReplicationEvent.Builder ev = Chat.ReplicationEvent.newBuilder()
                .setSender(serverName)
                .setType(type)
                .setId(id)
                .setTimestamp(ts)
                .setClock(incrementClock());

        if (user != null && !user.isEmpty()) ev.setUser(user);
        if (channel != null && !channel.isEmpty()) ev.setChannel(channel);
        if (content != null && !content.isEmpty()) ev.setContent(content);
        if (name != null && !name.isEmpty()) ev.setName(name);

        pubSocket.send("replication".getBytes(ZMQ.CHARSET), ZMQ.SNDMORE);
        pubSocket.send(ev.build().toByteArray(), 0);
    }

    private void listenPeer() {
        try (ZMQ.Socket peerRep = context.createSocket(SocketType.REP)) {
            peerRep.bind("tcp://0.0.0.0:6000");

            while (running) {
                byte[] raw = peerRep.recv(0);
                if (raw == null) continue;

                Chat.ServerMessage req = Chat.ServerMessage.parseFrom(raw);
                updateClock(req.getClock());

                Chat.ServerMessage.Builder rep = Chat.ServerMessage.newBuilder()
                        .setId(genId())
                        .setTimestamp(getTimestamp());

                if (req.hasClockSync()) {
                    rep.setClockRes(Chat.ClockSyncResponse.newBuilder()
                            .setServer(serverName)
                            .setPhysicalTime(getTimestamp())
                            .build());
                } else if (req.hasElection()) {
                    rep.setElectionRes(Chat.ElectionResponse.newBuilder()
                            .setServer(serverName)
                            .setStatus(true)
                            .build());
                }

                rep.setClock(incrementClock());
                peerRep.send(rep.build().toByteArray(), 0);
            }
        } catch (Exception ignored) {}
    }

    private void listenServersSub() {
        try (ZMQ.Socket sub = context.createSocket(SocketType.SUB)) {
            sub.connect(proxySubUri);
            sub.subscribe("servers".getBytes(ZMQ.CHARSET));

            while (running) {
                byte[] topic = sub.recv(0);
                byte[] data = sub.recv(0);
                if (data == null) continue;

                Chat.CoordinatorAnnouncement ann = Chat.CoordinatorAnnouncement.parseFrom(data);
                updateClock(ann.getClock());
                coordinator = ann.getCoordinator();

                System.out.println("[ELECTION] Coordenador atualizado via 'servers': " + coordinator
                        + " (rank " + ann.getRank() + ")");
            }
        } catch (Exception ignored) {}
    }

    private void listenReplication() {
        try (ZMQ.Socket sub = context.createSocket(SocketType.SUB)) {
            sub.connect(proxySubUri);
            sub.subscribe("replication".getBytes(ZMQ.CHARSET));

            while (running) {
                byte[] topic = sub.recv(0);
                byte[] data = sub.recv(0);
                if (data == null) continue;

                Chat.ReplicationEvent ev = Chat.ReplicationEvent.parseFrom(data);
                if (ev.getSender().equals(serverName)) continue;

                updateClock(ev.getClock());

                switch (ev.getType()) {
                    case REP_LOGIN:
                        db.saveLogin(ev.getId(), ev.getUser(), ev.getTimestamp());
                        break;
                    case REP_CHANNEL:
                        db.createChannel(ev.getId(), ev.getName(), ev.getTimestamp());
                        break;
                    case REP_MESSAGE:
                        db.saveMessage(ev.getId(), ev.getUser(), ev.getChannel(), ev.getContent(), ev.getTimestamp());
                        break;
                    default:
                        break;
                }

                System.out.println("[REPLICA] Sincronizado: tipo=" + ev.getType()
                        + " id=" + ev.getId() + " de=" + ev.getSender());
            }
        } catch (Exception ignored) {}
    }

    private void getLogin(Chat.LoginRequest req, long ts, Chat.Response.Builder res) {
        String user = req.getUser();
        Chat.LoginResponse.Builder loginRes = Chat.LoginResponse.newBuilder().setId(req.getId());
        if (db.saveLogin(req.getId(), user, ts)) {
            replicateEvent(Chat.ReplicationType.REP_LOGIN, req.getId(), user, "", "", "", ts);
            loginRes.setStatus(true).setMessage("Login Successfully.");
            System.out.println("[INFO] New session: " + user);
        } else {
            loginRes.setStatus(false).setMessage("Login Error.");
            System.out.println("[WARN] Login failed: " + user);
        }
        res.setLogin(loginRes);
    }

    private void createChannel(Chat.ChannelRequest req, long ts, Chat.Response.Builder res) {
        String name = req.getName();
        Chat.ChannelResponse.Builder chanRes = Chat.ChannelResponse.newBuilder()
                .setId(req.getId())
                .setName(name);

        if (db.channelExists(name)) {
            chanRes.setStatus(false).setMessage("Channel already exists.");
            System.out.println("[WARN] Channel already exists: " + name);
        } else if (db.createChannel(req.getId(), name, ts)) {
            replicateEvent(Chat.ReplicationType.REP_CHANNEL, req.getId(), "", "", "", name, ts);
            chanRes.setStatus(true).setMessage("Channel created.");
            System.out.println("[INFO] Channel created: " + name);
        } else {
            chanRes.setStatus(false).setMessage("Channel already exists.");
            System.out.println("[WARN] Channel already exists: " + name);
        }
        res.setChannel(chanRes);
    }

    private void listChannels(Chat.Response.Builder res) {
        Chat.ChannelListResponse.Builder listRes = Chat.ChannelListResponse.newBuilder();
        for (String ch : db.listChannels()) {
            listRes.addChannels(ch);
        }
        res.setList(listRes);
    }

    private void publishMessage(Chat.PublishRequest req, long ts, Chat.Response.Builder res) {
        Chat.PublishResponse.Builder pubRes = Chat.PublishResponse.newBuilder().setId(req.getId());
        if (db.saveMessage(req.getId(), req.getUser(), req.getChannel(), req.getContent(), ts)) {
            replicateEvent(Chat.ReplicationType.REP_MESSAGE, req.getId(), req.getUser(),
                    req.getChannel(), req.getContent(), "", ts);

            Chat.Publication pub = Chat.Publication.newBuilder()
                    .setId(req.getId())
                    .setUser(req.getUser())
                    .setChannel(req.getChannel())
                    .setContent(req.getContent())
                    .setTimestamp(getTimestamp())
                    .setClock(incrementClock())
                    .build();

            pubSocket.send(req.getChannel().getBytes(ZMQ.CHARSET), ZMQ.SNDMORE);
            pubSocket.send(pub.toByteArray(), 0);

            pubRes.setStatus(true).setMessage("Published OK.");
            System.out.println("[PUB] canal=" + req.getChannel() + " user=" + req.getUser()
                    + " msg=\"" + req.getContent() + "\" ts=" + pub.getTimestamp()
                    + " clock=" + pub.getClock());
        } else {
            pubRes.setStatus(false).setMessage("Publish Error.");
        }
        res.setPublish(pubRes);
    }

    public void run() {
        registerInReference();

        new Thread(this::listenPeer).start();
        new Thread(this::listenServersSub).start();
        new Thread(this::listenReplication).start();

        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {}

        startElection();

        while (running) {
            byte[] msg = repSocket.recv(0);
            if (msg == null) continue;

            try {
                Chat.Request req = Chat.Request.parseFrom(msg);
                updateClock(req.getClock());
                reqCount++;

                System.out.println("[RECV] id=" + req.getId() + " ts=" + req.getTimestamp()
                        + " clock=" + req.getClock());

                if (reqCount % 10 == 0) {
                    sendHeartbeat();
                }

                if (reqCount % 15 == 0) {
                    syncClockWithCoordinator();
                }

                Chat.Response.Builder res = Chat.Response.newBuilder()
                        .setId(genId())
                        .setTimestamp(getTimestamp());

                if (req.hasLogin()) getLogin(req.getLogin(), req.getTimestamp(), res);
                else if (req.hasChannel()) createChannel(req.getChannel(), req.getTimestamp(), res);
                else if (req.hasList()) listChannels(res);
                else if (req.hasPublish()) publishMessage(req.getPublish(), req.getTimestamp(), res);

                res.setClock(incrementClock());
                repSocket.send(res.build().toByteArray(), 0);

                System.out.println("[SEND] id=" + res.getId() + " ts=" + res.getTimestamp()
                        + " clock=" + res.getClock());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        String host = System.getenv("HOSTNAME");
        if (host == null || host.isEmpty()) host = "local";

        Server server = new Server(
                "tcp://broker:5556",
                "tcp://proxy:5557",
                "tcp://proxy:5558",
                "tcp://reference:5559",
                "srv_java_" + host,
                host,
                "/data/server_" + host + ".db"
        );
        server.run();
    }
}
