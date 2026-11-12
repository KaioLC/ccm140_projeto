package chat;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class Client {
    private final String user;
    private String userId;
    private long clock = 0;
    private final Set<String> subscribed = new HashSet<>();
    private final ZContext context;
    private final ZMQ.Socket reqSocket;
    private final ZMQ.Socket subSocket;
    private final Random random = new Random();

    public Client(String brokerUri, String proxyUri, String name) {
        this.user = name;
        this.context = new ZContext();
        this.reqSocket = context.createSocket(SocketType.REQ);
        this.reqSocket.connect(brokerUri);

        this.subSocket = context.createSocket(SocketType.SUB);
        this.subSocket.connect(proxyUri);

        this.userId = genId();
    }

    private long getTimestamp() {
        return System.currentTimeMillis();
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

    private Chat.Response sendReq(Chat.Request.Builder req) {
        req.setId(genId())
           .setTimestamp(getTimestamp())
           .setClock(incrementClock());

        Chat.Request request = req.build();
        reqSocket.send(request.toByteArray(), 0);
        System.out.println("[SEND] user=" + user + " id=" + request.getId()
                + " ts=" + request.getTimestamp() + " clock=" + request.getClock());

        byte[] reply = reqSocket.recv(0);
        try {
            Chat.Response res = Chat.Response.parseFrom(reply);
            updateClock(res.getClock());
            System.out.println("[RECV] user=" + user + " id=" + res.getId()
                    + " ts=" + res.getTimestamp() + " clock=" + clock);
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            return Chat.Response.getDefaultInstance();
        }
    }

    private void listenSub() {
        while (true) {
            byte[] topic = subSocket.recv(0);
            byte[] data = subSocket.recv(0);
            if (data == null) continue;

            try {
                Chat.Publication pub = Chat.Publication.parseFrom(data);
                updateClock(pub.getClock());
                long recvTs = getTimestamp();

                System.out.println("[SUB " + user + "] canal=" + pub.getChannel()
                        + " msg=\"" + pub.getContent() + "\""
                        + " ts_envio=" + pub.getTimestamp()
                        + " ts_recebimento=" + recvTs
                        + " clock=" + clock);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void login() {
        for (int attempt = 1; attempt <= 5; ++attempt) {
            Chat.Request.Builder req = Chat.Request.newBuilder()
                    .setLogin(Chat.LoginRequest.newBuilder()
                            .setId(userId)
                            .setUser(user)
                            .build());

            Chat.Response res = sendReq(req);
            if (res.getLogin().getStatus()) {
                System.out.println("[INFO] " + res.getLogin().getMessage());
                return;
            }

            System.out.println("[WARN] " + res.getLogin().getMessage()
                    + " Retrying (" + attempt + "/5)...");
            userId = genId();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ignored) {}
        }
    }

    public List<String> listChannels() {
        Chat.Request.Builder req = Chat.Request.newBuilder()
                .setList(Chat.ChannelListRequest.newBuilder().build());

        Chat.Response res = sendReq(req);
        return new ArrayList<>(res.getList().getChannelsList());
    }

    public void createChannel(String name) {
        Chat.Request.Builder req = Chat.Request.newBuilder()
                .setChannel(Chat.ChannelRequest.newBuilder()
                        .setId(genId())
                        .setName(name)
                        .build());

        Chat.Response res = sendReq(req);
        System.out.println("[INFO] Channel '" + name + "': " + res.getChannel().getMessage());
    }

    public synchronized void subscribeChannel(String channel) {
        if (subscribed.add(channel)) {
            subSocket.subscribe(channel.getBytes(ZMQ.CHARSET));
            System.out.println("[INFO] Subscribed to channel: " + channel);
        }
    }

    public void publish(String channel, String text) {
        Chat.Request.Builder req = Chat.Request.newBuilder()
                .setPublish(Chat.PublishRequest.newBuilder()
                        .setId(genId())
                        .setUser(user)
                        .setChannel(channel)
                        .setContent(text)
                        .build());

        Chat.Response res = sendReq(req);
        System.out.println("[INFO] Publish to '" + channel + "': " + res.getPublish().getMessage());
    }

    public void run() {
        login();

        Thread subThread = new Thread(this::listenSub);
        subThread.setDaemon(true);
        subThread.start();

        int count = 1;
        while (true) {
            List<String> channels = listChannels();

            if (channels.size() < 5) {
                createChannel("canal_" + user + "_" + count++);
                channels = listChannels();
            }

            if (subscribed.size() < 3 && !channels.isEmpty()) {
                for (String ch : channels) {
                    if (!subscribed.contains(ch)) {
                        subscribeChannel(ch);
                        break;
                    }
                }
            }

            if (!channels.isEmpty()) {
                String target = channels.get(random.nextInt(channels.size()));
                for (int i = 1; i <= 10; ++i) {
                    publish(target, "Msg " + i + " from " + user);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {}
                }
            }
        }
    }

    public static void main(String[] args) {
        String host = System.getenv("HOSTNAME");
        if (host == null || host.isEmpty()) host = "local";

        Client client = new Client(
                "tcp://broker:5555",
                "tcp://proxy:5558",
                "bot_java_" + host
        );
        client.run();
    }
}
