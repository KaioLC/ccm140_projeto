package chat;

import com.google.protobuf.InvalidProtocolBufferException;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ClienteBot {
    private static final int MAX_TENTATIVAS_LOGIN = 5;
    private static final int MAX_CANAIS = 5;
    private static final int MAX_INSCRICOES = 3;
    private static final int MENSAGENS_POR_CICLO = 10;

    private final String nomeUsuario;
    private final String enderecoBroker;
    private final String enderecoProxy;
    private final RelogioLamport relogio = new RelogioLamport();
    private final Object sincronizadorRelogio = new Object();
    private final BlockingQueue<String> filaInscricoes = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> confirmacoesInscricao = new LinkedBlockingQueue<>();
    private final java.util.Set<String> canaisInscritos = java.util.concurrent.ConcurrentHashMap
            .newKeySet();
    private final AtomicBoolean executando = new AtomicBoolean(true);

    private ClienteBot(String nomeUsuario, String enderecoBroker, String enderecoProxy) {
        this.nomeUsuario = nomeUsuario;
        this.enderecoBroker = enderecoBroker;
        this.enderecoProxy = enderecoProxy;
    }

    private Chat.Response trocarMensagem(ZMQ.Socket soquete, Chat.Request requisicao)
            throws InvalidProtocolBufferException {
        synchronized (sincronizadorRelogio) {
            requisicao = requisicao.toBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setClock(relogio.antesDeEnviar())
                    .build();
            RegistroProtocolo.registrarMensagem("ENVIO", nomeUsuario, requisicao);
            if (!soquete.send(requisicao.toByteArray(), 0)) {
                throw new IllegalStateException("ZeroMQ nao conseguiu enviar a requisicao");
            }
        }

        byte[] bytesResposta = soquete.recv(0);
        if (bytesResposta == null) {
            throw new IllegalStateException("ZeroMQ nao recebeu uma resposta");
        }

        Chat.Response resposta = Chat.Response.parseFrom(bytesResposta);
        synchronized (sincronizadorRelogio) {
            relogio.aoReceber(resposta.getClock());
            RegistroProtocolo.registrarMensagem("RECEBIMENTO", nomeUsuario, resposta);
        }
        return resposta;
    }

    private void autenticar(ZMQ.Socket soquete)
            throws InterruptedException, InvalidProtocolBufferException {
        for (int tentativa = 1; tentativa <= MAX_TENTATIVAS_LOGIN; tentativa++) {
            String id = gerarId();
            Chat.Request requisicao = Chat.Request.newBuilder()
                    .setId(id)
                    .setLogin(Chat.LoginRequest.newBuilder().setId(id).setUser(nomeUsuario))
                    .build();

            Chat.Response resposta = trocarMensagem(soquete, requisicao);
            if (!resposta.hasLogin()) {
                throw new IllegalStateException(
                        "A resposta de login nao contem os dados esperados: " + resposta);
            }
            if (resposta.getLogin().getStatus()) {
                System.out.printf("[INFO][%s] %s%n",
                        nomeUsuario, resposta.getLogin().getMessage());
                return;
            }

            System.err.printf("[AVISO][%s] Tentativa de login %d/%d falhou: %s%n",
                    nomeUsuario, tentativa, MAX_TENTATIVAS_LOGIN,
                    resposta.getLogin().getMessage());
            if (tentativa < MAX_TENTATIVAS_LOGIN) {
                Thread.sleep(1_000L);
            }
        }
        throw new IllegalStateException(
                "O login falhou apos " + MAX_TENTATIVAS_LOGIN + " tentativas");
    }

    private List<String> listarCanais(ZMQ.Socket soquete)
            throws InvalidProtocolBufferException {
        Chat.Request requisicao = Chat.Request.newBuilder()
                .setId(gerarId())
                .setList(Chat.ChannelListRequest.getDefaultInstance())
                .build();

        Chat.Response resposta = trocarMensagem(soquete, requisicao);
        if (!resposta.hasList()) {
            throw new IllegalStateException(
                    "A resposta nao contem a lista de canais: " + resposta);
        }
        return new ArrayList<>(resposta.getList().getChannelsList());
    }

    private boolean criarCanal(ZMQ.Socket soquete, String nome)
            throws InvalidProtocolBufferException {
        String id = gerarId();
        Chat.Request requisicao = Chat.Request.newBuilder()
                .setId(id)
                .setChannel(Chat.ChannelRequest.newBuilder().setId(id).setName(nome))
                .build();

        Chat.Response resposta = trocarMensagem(soquete, requisicao);
        if (!resposta.hasChannel()) {
            throw new IllegalStateException(
                    "A resposta nao contem os dados do canal: " + resposta);
        }
        System.out.printf("[INFO][%s] %s%n",
                nomeUsuario, resposta.getChannel().getMessage());
        return resposta.getChannel().getStatus();
    }

    private void publicar(ZMQ.Socket soquete, String canal, int numero)
            throws InvalidProtocolBufferException {
        String id = gerarId();
        Chat.Request requisicao = Chat.Request.newBuilder()
                .setId(id)
                .setPublish(Chat.PublishRequest.newBuilder()
                        .setId(id)
                        .setUser(nomeUsuario)
                        .setChannel(canal)
                        .setContent("Mensagem " + numero + " de " + nomeUsuario))
                .build();

        Chat.Response resposta = trocarMensagem(soquete, requisicao);
        if (!resposta.hasPublish()) {
            throw new IllegalStateException(
                    "A resposta de publicacao nao tem o formato esperado: " + resposta);
        }
        System.out.printf("[INFO][%s] %s%n",
                nomeUsuario, resposta.getPublish().getMessage());
    }

    private void iniciarEscutaPublicacoes() {
        // O socket SUB pertence a esta thread; o fluxo REQ roda na thread principal.
        Thread threadEscuta = new Thread(this::escutarPublicacoes,
                "escuta-publicacoes-" + nomeUsuario);
        threadEscuta.setDaemon(true);
        threadEscuta.start();
    }

    private void escutarPublicacoes() {
        try (ZContext contexto = new ZContext()) {
            ZMQ.Socket soquete = contexto.createSocket(SocketType.SUB);
            soquete.setLinger(0);
            soquete.setReceiveTimeOut(250);
            soquete.connect(enderecoProxy);

            while (executando.get()) {
                String canal = filaInscricoes.poll(100, TimeUnit.MILLISECONDS);
                if (canal != null) {
                    soquete.subscribe(canal.getBytes(StandardCharsets.UTF_8));
                    System.out.printf("[INFO][%s] Inscrito no canal %s%n", nomeUsuario, canal);
                    confirmacoesInscricao.offer(canal);
                }

                byte[] quadroCanal = soquete.recv(0);
                if (quadroCanal == null) {
                    continue;
                }
                byte[] quadroPublicacao = soquete.recv(0);
                if (quadroPublicacao == null) {
                    System.err.printf("[ERRO][%s] Publicacao sem o quadro de conteudo.%n",
                            nomeUsuario);
                    continue;
                }

                Chat.Publication publicacao = Chat.Publication.parseFrom(quadroPublicacao);
                synchronized (sincronizadorRelogio) {
                    relogio.aoReceber(publicacao.getClock());
                    RegistroProtocolo.registrarMensagem("RECEBIMENTO", nomeUsuario, publicacao);
                }
            }
            soquete.close();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (InvalidProtocolBufferException exception) {
            System.err.printf("[ERRO][%s] Publicacao Protobuf invalida: %s%n",
                    nomeUsuario, exception.getMessage());
        }
    }

    private void atualizarInscricoes(List<String> canais) throws InterruptedException {
        List<String> novasInscricoes = new ArrayList<>();
        for (String canal : canais) {
            if (canaisInscritos.size() >= MAX_INSCRICOES) {
                break;
            }
            if (canaisInscritos.add(canal)) {
                filaInscricoes.add(canal);
                novasInscricoes.add(canal);
            }
        }
        for (String canal : novasInscricoes) {
            String confirmado = confirmacoesInscricao.poll(5, TimeUnit.SECONDS);
            if (!canal.equals(confirmado)) {
                throw new IllegalStateException(
                        "A inscricao no canal " + canal + " nao foi confirmada pela thread SUB");
            }
        }
        if (!novasInscricoes.isEmpty()) {
            Thread.sleep(300L);
        }
    }

    private void executarFluxo() throws Exception {
        iniciarEscutaPublicacoes();
        try (ZContext contexto = new ZContext()) {
            ZMQ.Socket soquete = contexto.createSocket(SocketType.REQ);
            soquete.setLinger(0);
            soquete.connect(enderecoBroker);
            autenticar(soquete);

            List<String> canais = listarCanais(soquete);
            System.out.printf("[INFO][%s] Canais disponiveis nesta instancia: %s%n",
                    nomeUsuario, canais);
            if (canais.size() < MAX_CANAIS) {
                String nomeCanal = "java_" + UUID.randomUUID().toString().substring(0, 8);
                if (criarCanal(soquete, nomeCanal)) {
                    canais.add(nomeCanal);
                }
            }

            atualizarInscricoes(canais);
            while (executando.get()) {
                for (String canal : listarCanais(soquete)) {
                    if (!canais.contains(canal)) {
                        canais.add(canal);
                    }
                }
                atualizarInscricoes(canais);
                if (canais.isEmpty()) {
                    Thread.sleep(1_000L);
                    continue;
                }

                String canal = canais.get(ThreadLocalRandom.current().nextInt(canais.size()));
                for (int numero = 1; numero <= MENSAGENS_POR_CICLO; numero++) {
                    publicar(soquete, canal, numero);
                    Thread.sleep(1_000L);
                }
            }
            soquete.close();
        }
    }

    private static String gerarId() {
        return UUID.randomUUID().toString();
    }

    public static void main(String[] args) throws Exception {
        String enderecoBroker = System.getenv().getOrDefault(
                "BROKER_ENDPOINT", "tcp://localhost:5555");
        String enderecoProxy = System.getenv().getOrDefault(
                "PROXY_ENDPOINT", "tcp://localhost:5558");
        String nomeHost = System.getenv().getOrDefault(
                "HOSTNAME", InetAddress.getLocalHost().getHostName());
        String nomeUsuario = System.getenv().getOrDefault(
                "BOT_NAME", "bot_java_" + nomeHost);

        ClienteBot bot = new ClienteBot(nomeUsuario, enderecoBroker, enderecoProxy);
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> bot.executando.set(false), "encerramento-" + nomeUsuario));
        bot.executarFluxo();
    }
}
