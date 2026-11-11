package chat;

import com.google.protobuf.InvalidProtocolBufferException;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class Servidor {
    private static final String TOPICO_REPLICACAO = "replication";
    private static final String TOPICO_SERVIDORES = "servers";
    private static final int PORTA_SERVIDORES = 6000;
    private static final int TIMEOUT_SOQUETE_MS = 2_000;
    private static final int MENSAGENS_POR_BATIMENTO = 10;
    private static final int MENSAGENS_POR_SINCRONIZACAO = 15;

    private final String nome;
    private final String endpointProprio;
    private final int portaServidores;
    private final String enderecoReferencia;
    private final Persistencia persistencia;
    private final RelogioLamport relogio = new RelogioLamport();
    private final Object sincronizadorRelogio = new Object();
    private final AtomicLong diferencaRelogioFisico = new AtomicLong();
    private final AtomicInteger rank = new AtomicInteger();
    private final AtomicInteger requisicoesProcessadas = new AtomicInteger();
    private final AtomicReference<String> coordenador = new AtomicReference<>();
    private final AtomicBoolean executando = new AtomicBoolean(true);
    private final Map<String, Chat.ServerInfo> servidores = new ConcurrentHashMap<>();
    private final BlockingQueue<MensagemPublicacao> filaPublicacoes = new LinkedBlockingQueue<>();
    private final BlockingQueue<Boolean> filaBatimentos = new LinkedBlockingQueue<>();
    private final BlockingQueue<Boolean> filaSincronizacao = new LinkedBlockingQueue<>();
    private final BlockingQueue<Boolean> filaEleicao = new LinkedBlockingQueue<>();
    private final CountDownLatch assinantePronto = new CountDownLatch(1);
    private final CountDownLatch publicadorPronto = new CountDownLatch(1);
    private final CountDownLatch atendimentoPronto = new CountDownLatch(1);
    private final AtomicReference<RuntimeException> falhaInicializacaoSockets =
            new AtomicReference<>();

    private Servidor(String nome, String endpointProprio, int portaServidores,
                     String enderecoReferencia,
                     Persistencia persistencia) {
        this.nome = nome;
        this.endpointProprio = endpointProprio;
        this.portaServidores = portaServidores;
        this.enderecoReferencia = enderecoReferencia;
        this.persistencia = persistencia;
    }

    private void iniciar() throws Exception {
        iniciarAssinante();
        iniciarPublicador();
        iniciarAtendimentoEntreServidores();
        if (!assinantePronto.await(10, TimeUnit.SECONDS)
                || !publicadorPronto.await(10, TimeUnit.SECONDS)
                || !atendimentoPronto.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Tempo esgotado ao iniciar sockets do servidor");
        }
        if (falhaInicializacaoSockets.get() != null) {
            throw new IllegalStateException(
                    "Falha ao iniciar sockets do servidor", falhaInicializacaoSockets.get());
        }
        Thread.sleep(250L);

        registrarNoNoDeReferencia();
        atualizarListaServidores();
        iniciarComunicacaoNoDeReferencia();
        iniciarEleicao();
        executarRequisicoesDeClientes();
    }

    private void iniciarAssinante() {
        iniciarThread("assinante-" + nome, this::executarAssinante);
    }

    private void executarAssinante() {
        try (ZContext contexto = new ZContext()) {
            ZMQ.Socket soquete = contexto.createSocket(SocketType.SUB);
            soquete.setLinger(0);
            soquete.setReceiveTimeOut(500);
            soquete.connect(System.getenv().getOrDefault(
                    "PROXY_ENDPOINT", "tcp://localhost:5558"));
            soquete.subscribe(TOPICO_REPLICACAO.getBytes(StandardCharsets.UTF_8));
            soquete.subscribe(TOPICO_SERVIDORES.getBytes(StandardCharsets.UTF_8));
            assinantePronto.countDown();

            while (executando.get()) {
                byte[] topicoBytes = soquete.recv(0);
                if (topicoBytes == null) {
                    continue;
                }
                byte[] conteudo = soquete.recv(0);
                if (conteudo == null) {
                    System.err.printf("[ERRO][%s] Publicacao incompleta no proxy.%n", nome);
                    continue;
                }
                processarPublicacaoRecebida(
                        new String(topicoBytes, StandardCharsets.UTF_8), conteudo);
            }
        } catch (RuntimeException exception) {
            falhaInicializacaoSockets.compareAndSet(null, exception);
            if (executando.get()) {
                System.err.printf("[ERRO][%s] Thread SUB falhou: %s%n", nome,
                        exception.getMessage());
                exception.printStackTrace(System.err);
            }
        } finally {
            assinantePronto.countDown();
        }
    }

    private void processarPublicacaoRecebida(String topico, byte[] conteudo) {
        try {
            if (TOPICO_REPLICACAO.equals(topico)) {
                Chat.ReplicationEvent evento = Chat.ReplicationEvent.parseFrom(conteudo);
                receberRelogio(evento.getClock());
                RegistroProtocolo.registrarMensagem("RECEBIMENTO", nome, evento);
                if (!nome.equals(evento.getSender())) {
                    persistencia.aplicarEvento(evento);
                }
            } else if (TOPICO_SERVIDORES.equals(topico)) {
                Chat.CoordinatorAnnouncement anuncio =
                        Chat.CoordinatorAnnouncement.parseFrom(conteudo);
                receberRelogio(anuncio.getClock());
                RegistroProtocolo.registrarMensagem("RECEBIMENTO", nome, anuncio);
                coordenador.set(anuncio.getCoordinator());
                System.out.printf("[COORDENADOR][%s] Novo coordenador: %s (rank %d)%n",
                        nome, anuncio.getCoordinator(), anuncio.getRank());
            } else {
                Chat.Publication publicacao = Chat.Publication.parseFrom(conteudo);
                receberRelogio(publicacao.getClock());
                RegistroProtocolo.registrarMensagem("RECEBIMENTO", nome, publicacao);
            }
        } catch (InvalidProtocolBufferException | SQLException exception) {
            System.err.printf("[ERRO][%s] Falha ao processar topico %s: %s%n",
                    nome, topico, exception.getMessage());
            exception.printStackTrace(System.err);
        }
    }

    private void iniciarPublicador() {
        iniciarThread("publicador-" + nome, () -> {
            try (ZContext contexto = new ZContext()) {
                ZMQ.Socket soquete = contexto.createSocket(SocketType.PUB);
                soquete.setLinger(0);
                soquete.connect(System.getenv().getOrDefault(
                        "PROXY_PUBLISH_ENDPOINT", "tcp://localhost:5557"));
                publicadorPronto.countDown();
                while (executando.get()) {
                    MensagemPublicacao publicacao = filaPublicacoes.poll(
                            500, TimeUnit.MILLISECONDS);
                    if (publicacao == null) {
                        continue;
                    }
                    RegistroProtocolo.registrarQuadroBinario(
                            "ENVIO", nome + "/" + publicacao.topico, publicacao.conteudo);
                    if (!soquete.send(publicacao.topico.getBytes(StandardCharsets.UTF_8),
                            ZMQ.SNDMORE)
                            || !soquete.send(publicacao.conteudo, 0)) {
                        throw new IllegalStateException("ZeroMQ nao conseguiu publicar mensagem");
                    }
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException exception) {
                falhaInicializacaoSockets.compareAndSet(null, exception);
                if (executando.get()) {
                    System.err.printf("[ERRO][%s] Thread PUB falhou: %s%n",
                            nome, exception.getMessage());
                    exception.printStackTrace(System.err);
                }
            } finally {
                publicadorPronto.countDown();
            }
        });
    }

    private void iniciarAtendimentoEntreServidores() {
        iniciarThread("servidor-rep-" + nome, () -> {
            try (ZContext contexto = new ZContext()) {
                ZMQ.Socket soquete = contexto.createSocket(SocketType.REP);
                soquete.setLinger(0);
                soquete.bind("tcp://*:" + portaServidores);
                atendimentoPronto.countDown();
                atenderServidores(soquete);
            } catch (RuntimeException exception) {
                falhaInicializacaoSockets.compareAndSet(null, exception);
                atendimentoPronto.countDown();
                if (executando.get()) {
                    System.err.printf("[ERRO][%s] REP entre servidores falhou: %s%n",
                            nome, exception.getMessage());
                    exception.printStackTrace(System.err);
                }
            }
        });
    }

    private void atenderServidores(ZMQ.Socket soquete) {
        while (executando.get()) {
            byte[] bytes = soquete.recv(0);
            if (bytes == null) {
                continue;
            }
            Chat.ServerMessage resposta;
            try {
                Chat.ServerMessage requisicao = Chat.ServerMessage.parseFrom(bytes);
                receberRelogio(requisicao.getClock());
                RegistroProtocolo.registrarMensagem("RECEBIMENTO", nome, requisicao);
                resposta = processarMensagemServidor(requisicao);
            } catch (InvalidProtocolBufferException exception) {
                System.err.printf("[ERRO][%s] Mensagem entre servidores invalida: %s%n",
                        nome, exception.getMessage());
                resposta = novaMensagem(builder -> builder.setElectionRes(
                        Chat.ElectionResponse.newBuilder().setServer(nome).setStatus(false)));
            }
            RegistroProtocolo.registrarMensagem("ENVIO", nome, resposta);
            if (!soquete.send(resposta.toByteArray(), 0)) {
                throw new IllegalStateException("ZeroMQ nao conseguiu responder ao servidor");
            }
        }
    }

    private Chat.ServerMessage processarMensagemServidor(Chat.ServerMessage requisicao) {
        if (requisicao.hasClockSync()) {
            long horaAlvo = requisicao.getClockSync().getTargetTime();
            if (horaAlvo > 0) {
                diferencaRelogioFisico.set(horaAlvo - System.currentTimeMillis());
            }
            long horaFinal = timestampAtual();
            return novaMensagem(builder -> builder.setClockRes(
                    Chat.ClockSyncResponse.newBuilder()
                            .setServer(nome)
                            .setPhysicalTime(horaFinal)));
        }
        if (requisicao.hasElection()) {
            Chat.ElectionRequest eleicao = requisicao.getElection();
            filaEleicao.offer(Boolean.TRUE);
            return novaMensagem(builder -> builder.setElectionRes(
                    Chat.ElectionResponse.newBuilder().setServer(nome).setStatus(true)));
        }
        if (requisicao.hasReplicationSync()) {
            try {
                List<Chat.ReplicationEvent> eventos = persistencia.listarEventos(nome);
                return novaMensagem(builder -> builder.setReplicationSnapshot(
                        Chat.ReplicationSyncResponse.newBuilder().addAllEvents(eventos)));
            } catch (SQLException exception) {
                System.err.printf("[ERRO][%s] Falha ao preparar snapshot: %s%n",
                        nome, exception.getMessage());
                return novaMensagem(builder -> builder.setReplicationSnapshot(
                        Chat.ReplicationSyncResponse.getDefaultInstance()));
            }
        }
        return novaMensagem(builder -> builder.setElectionRes(
                Chat.ElectionResponse.newBuilder().setServer(nome).setStatus(false)));
    }

    private Chat.Response processarRequisicaoCliente(Chat.Request requisicao) {
        Chat.Response.Builder resposta = Chat.Response.newBuilder().setId(gerarId());
        if (requisicao.hasLogin()) {
            Chat.LoginRequest login = requisicao.getLogin();
            boolean salvo = false;
            try {
                Chat.ReplicationEvent evento = criarEvento(
                        Chat.ReplicationType.REP_LOGIN, login.getId(),
                        login.getUser(), "", "", "");
                salvo = persistencia.aplicarEvento(evento);
                if (salvo) {
                    publicarEventoReplicacao(evento);
                }
            } catch (SQLException exception) {
                registrarFalhaPersistencia("login", exception);
            }
            resposta.setLogin(Chat.LoginResponse.newBuilder()
                    .setId(login.getId())
                    .setStatus(salvo)
                    .setMessage(salvo ? "Login realizado com sucesso." : "Falha no login."));
        } else if (requisicao.hasChannel()) {
            Chat.ChannelRequest canal = requisicao.getChannel();
            boolean criado = false;
            String mensagem;
            try {
                Chat.ReplicationEvent evento = criarEvento(
                        Chat.ReplicationType.REP_CHANNEL, canal.getId(),
                        "", canal.getName(), "", "");
                criado = persistencia.aplicarEvento(evento);
                if (criado) {
                    publicarEventoReplicacao(evento);
                    mensagem = "Canal criado.";
                } else {
                    mensagem = "Esse canal ja existe.";
                }
            } catch (SQLException exception) {
                registrarFalhaPersistencia("criacao do canal", exception);
                mensagem = "Falha ao criar o canal.";
            }
            resposta.setChannel(Chat.ChannelResponse.newBuilder()
                    .setId(canal.getId())
                    .setName(canal.getName())
                    .setStatus(criado)
                    .setMessage(mensagem));
        } else if (requisicao.hasList()) {
            try {
                resposta.setList(Chat.ChannelListResponse.newBuilder()
                        .addAllChannels(persistencia.listarCanais()));
            } catch (SQLException exception) {
                registrarFalhaPersistencia("listagem de canais", exception);
            }
        } else if (requisicao.hasPublish()) {
            processarPublicacao(requisicao, resposta);
        }
        return resposta.build();
    }

    private void processarPublicacao(Chat.Request requisicao, Chat.Response.Builder resposta) {
        Chat.PublishRequest publicacao = requisicao.getPublish();
        boolean salva = false;
        String mensagem;
        try {
            Chat.ReplicationEvent evento = criarEvento(
                    Chat.ReplicationType.REP_MESSAGE, publicacao.getId(),
                    publicacao.getUser(), "", publicacao.getChannel(), publicacao.getContent());
            salva = persistencia.aplicarEvento(evento);
            if (salva) {
                publicarEventoReplicacao(evento);
                publicarMensagemChat(publicacao);
                mensagem = "Mensagem publicada.";
            } else {
                mensagem = "Publicacao duplicada ou nao salva.";
            }
        } catch (SQLException exception) {
            registrarFalhaPersistencia("publicacao", exception);
            mensagem = "Falha ao persistir a publicacao.";
        }
        resposta.setPublish(Chat.PublishResponse.newBuilder()
                .setId(publicacao.getId())
                .setStatus(salva)
                .setMessage(mensagem));
    }

    private Chat.ReplicationEvent criarEvento(Chat.ReplicationType tipo, String id,
                                              String usuario, String nomeRegistro,
                                              String canal, String conteudo) {
        long clock;
        long timestamp;
        synchronized (sincronizadorRelogio) {
            clock = relogio.antesDeEnviar();
            timestamp = timestampAtual();
        }
        return Chat.ReplicationEvent.newBuilder()
                .setSender(nome)
                .setType(tipo)
                .setId(id)
                .setTimestamp(timestamp)
                .setClock(clock)
                .setUser(usuario)
                .setName(nomeRegistro)
                .setChannel(canal)
                .setContent(conteudo)
                .build();
    }

    private void publicarEventoReplicacao(Chat.ReplicationEvent evento) {
        RegistroProtocolo.registrarMensagem("ENVIO", nome, evento);
        filaPublicacoes.offer(new MensagemPublicacao(
                TOPICO_REPLICACAO, evento.toByteArray()));
    }

    private void publicarMensagemChat(Chat.PublishRequest requisicao) {
        Chat.Publication publicacao;
        synchronized (sincronizadorRelogio) {
            publicacao = Chat.Publication.newBuilder()
                    .setId(requisicao.getId())
                    .setUser(requisicao.getUser())
                    .setChannel(requisicao.getChannel())
                    .setContent(requisicao.getContent())
                    .setTimestamp(timestampAtual())
                    .setClock(relogio.antesDeEnviar())
                    .build();
        }
        RegistroProtocolo.registrarMensagem("ENVIO", nome, publicacao);
        filaPublicacoes.offer(new MensagemPublicacao(
                requisicao.getChannel(), publicacao.toByteArray()));
    }

    private void executarRequisicoesDeClientes() throws InvalidProtocolBufferException {
        try (ZContext contexto = new ZContext()) {
            ZMQ.Socket soquete = contexto.createSocket(SocketType.REP);
            soquete.setLinger(0);
            soquete.connect(System.getenv().getOrDefault(
                    "BROKER_ENDPOINT", "tcp://localhost:5556"));
            System.out.printf("[SERVIDOR][%s] rank=%d coordenador=%s%n",
                    nome, rank.get(), coordenador.get());

            while (executando.get()) {
                byte[] bytes = soquete.recv(0);
                if (bytes == null) {
                    continue;
                }
                Chat.Response resposta;
                try {
                    Chat.Request requisicao = Chat.Request.parseFrom(bytes);
                    receberRelogio(requisicao.getClock());
                    RegistroProtocolo.registrarMensagem("RECEBIMENTO", nome, requisicao);
                    resposta = processarRequisicaoCliente(requisicao);
                } catch (InvalidProtocolBufferException exception) {
                    System.err.printf("[ERRO][%s] Requisicao Protobuf invalida: %s%n",
                            nome, exception.getMessage());
                    resposta = Chat.Response.newBuilder().setId("requisicao-invalida").build();
                }

                synchronized (sincronizadorRelogio) {
                    resposta = resposta.toBuilder()
                            .setTimestamp(timestampAtual())
                            .setClock(relogio.antesDeEnviar())
                            .build();
                    RegistroProtocolo.registrarMensagem("ENVIO", nome, resposta);
                    if (!soquete.send(resposta.toByteArray(), 0)) {
                        throw new IllegalStateException(
                                "ZeroMQ nao conseguiu responder ao cliente");
                    }
                }
                int quantidade = requisicoesProcessadas.incrementAndGet();
                if (quantidade % MENSAGENS_POR_BATIMENTO == 0) {
                    filaBatimentos.offer(Boolean.TRUE);
                }
                if (quantidade % MENSAGENS_POR_SINCRONIZACAO == 0) {
                    filaSincronizacao.offer(Boolean.TRUE);
                }
            }
        }
    }

    private void iniciarComunicacaoNoDeReferencia() {
        iniciarThread("referencia-" + nome, this::executarComunicacaoReferencia);
        iniciarThread("sincronizacao-relogio-" + nome, this::executarSincronizacaoAgendada);
        iniciarThread("replicacao-anti-entropia-" + nome, this::executarAntiEntropia);
        iniciarThread("eleicao-" + nome, this::executarEleicoes);
    }

    private void registrarNoNoDeReferencia() throws Exception {
        Chat.ReferenceRequest requisicao = novaRequisicaoReferencia(builder ->
                builder.setReg(Chat.RegisterRequest.newBuilder()
                        .setName(nome)
                        .setEndpoint(endpointProprio)));
        Chat.ReferenceResponse resposta = enviarReferencia(requisicao);
        if (!resposta.hasReg()) {
            throw new IllegalStateException("No de Referencia nao retornou o rank");
        }
        rank.set(resposta.getReg().getRank());
        System.out.printf("[SERVIDOR][%s] Registrado no No de Referencia com rank %d%n",
                nome, rank.get());
    }

    private void atualizarListaServidores() throws Exception {
        Chat.ReferenceResponse resposta = enviarReferencia(
                novaRequisicaoReferencia(builder ->
                        builder.setList(Chat.ServerListRequest.getDefaultInstance())));
        if (!resposta.hasList()) {
            throw new IllegalStateException("No de Referencia nao retornou a lista de servidores");
        }
        servidores.clear();
        for (Chat.ServerInfo servidor : resposta.getList().getServersList()) {
            servidores.put(servidor.getName(), servidor);
        }
        reconciliarCoordenadorComServidoresAtivos();
    }

    private void reconciliarCoordenadorComServidoresAtivos() {
        String atual = coordenador.get();
        if (atual != null && !servidores.containsKey(atual)) {
            coordenador.compareAndSet(atual, null);
            atual = coordenador.get();
        }
        if (atual == null) {
            Chat.ServerInfo maior = servidores.values().stream()
                    .max((a, b) -> Integer.compare(a.getRank(), b.getRank()))
                    .orElse(null);
            if (maior != null) {
                coordenador.set(maior.getName());
                if (maior.getName().equals(nome)) {
                    anunciarCoordenador();
                }
            }
        }
    }

    private void anunciarNovamenteSeCoordenadorDeMaiorRank() {
        Chat.ServerInfo maior = servidores.values().stream()
                .max((a, b) -> Integer.compare(a.getRank(), b.getRank()))
                .orElse(null);
        if (maior != null && maior.getName().equals(nome)
                && nome.equals(coordenador.get())) {
            anunciarCoordenador();
        }
    }

    private void executarComunicacaoReferencia() {
        while (executando.get()) {
            try {
                filaBatimentos.poll(10, TimeUnit.SECONDS);
                Chat.ReferenceResponse resposta = enviarReferencia(
                        novaRequisicaoReferencia(builder ->
                                builder.setHeartbeat(Chat.HeartbeatRequest.newBuilder()
                                        .setName(nome))));
                if (!resposta.hasHeartbeat() || !resposta.getHeartbeat().getStatus()) {
                    registrarNoNoDeReferencia();
                }
                atualizarListaServidores();
                anunciarNovamenteSeCoordenadorDeMaiorRank();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception exception) {
                System.err.printf("[AVISO][%s] Falha temporaria no No de Referencia: %s%n",
                        nome, exception.getMessage());
            }
        }
    }

    private void executarSincronizacaoAgendada() {
        while (executando.get()) {
            try {
                filaSincronizacao.take();
                sincronizarComCoordenador();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception exception) {
                System.err.printf("[AVISO][%s] Sincronizacao Berkeley falhou: %s%n",
                        nome, exception.getMessage());
                filaEleicao.offer(Boolean.TRUE);
            }
        }
    }

    private void sincronizarComCoordenador() throws Exception {
        if (nome.equals(coordenador.get())) {
            sincronizarBerkeley();
            return;
        }
        Chat.ServerInfo info = servidores.get(coordenador.get());
        if (info == null) {
            atualizarListaServidores();
            info = servidores.get(coordenador.get());
        }
        if (info == null || info.getEndpoint().isBlank()) {
            throw new IllegalStateException("Endpoint do coordenador nao esta disponivel");
        }
        Chat.ServerMessage resposta = enviarServidor(info.getEndpoint(),
                novaMensagem(builder -> builder.setClockSync(
                        Chat.ClockSyncRequest.newBuilder().setServer(nome))));
        if (!resposta.hasClockRes()) {
            throw new IllegalStateException("Coordenador nao retornou hora para Berkeley");
        }
        diferencaRelogioFisico.set(
                resposta.getClockRes().getPhysicalTime() - System.currentTimeMillis());
        System.out.printf("[RELOGIO][%s] Berkeley: hora=%d ajuste=%d ms%n",
                nome, timestampAtual(), diferencaRelogioFisico.get());
    }

    private long sincronizarBerkeley() {
        List<Chat.ServerInfo> participantes = new ArrayList<>(servidores.values());
        List<Chat.ServerInfo> sincronizados = new ArrayList<>();
        List<Long> horas = new ArrayList<>();
        long horaLocal = System.currentTimeMillis();
        horas.add(horaLocal);

        for (Chat.ServerInfo servidor : participantes) {
            if (servidor.getName().equals(nome) || servidor.getEndpoint().isBlank()) {
                continue;
            }
            try {
                Chat.ServerMessage resposta = enviarServidor(servidor.getEndpoint(),
                        novaMensagem(builder -> builder.setClockSync(
                                Chat.ClockSyncRequest.newBuilder().setServer(nome))));
                if (resposta.hasClockRes()) {
                    sincronizados.add(servidor);
                    horas.add(resposta.getClockRes().getPhysicalTime());
                }
            } catch (Exception exception) {
                System.err.printf("[AVISO][%s] Nao foi possivel consultar %s: %s%n",
                        nome, servidor.getName(), exception.getMessage());
            }
        }

        long media = (long) horas.stream().mapToLong(Long::longValue)
                .average().orElse(horaLocal);
        diferencaRelogioFisico.set(media - System.currentTimeMillis());

        for (Chat.ServerInfo servidor : sincronizados) {
            try {
                enviarServidor(servidor.getEndpoint(),
                        novaMensagem(builder -> builder.setClockSync(
                                Chat.ClockSyncRequest.newBuilder()
                                        .setServer(nome)
                                        .setTargetTime(media))));
            } catch (Exception exception) {
                System.err.printf("[AVISO][%s] Ajuste Berkeley nao chegou a %s: %s%n",
                        nome, servidor.getName(), exception.getMessage());
            }
        }
        System.out.printf("[RELOGIO][%s] Coordenador Berkeley definiu hora media %d%n",
                nome, media);
        return media;
    }

    private void executarAntiEntropia() {
        while (executando.get()) {
            try {
                Thread.sleep(10_000L);
                atualizarListaServidores();
                for (Chat.ServerInfo servidor : servidores.values()) {
                    if (servidor.getName().equals(nome) || servidor.getEndpoint().isBlank()) {
                        continue;
                    }
                    Chat.ServerMessage resposta = enviarServidor(servidor.getEndpoint(),
                            novaMensagem(builder -> builder.setReplicationSync(
                                    Chat.ReplicationSyncRequest.getDefaultInstance())));
                    if (resposta.hasReplicationSnapshot()) {
                        for (Chat.ReplicationEvent evento
                                : resposta.getReplicationSnapshot().getEventsList()) {
                            receberRelogio(evento.getClock());
                            if (!nome.equals(evento.getSender())) {
                                persistencia.aplicarEvento(evento);
                            }
                        }
                    }
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception exception) {
                System.err.printf("[AVISO][%s] Anti-entropia sera repetida: %s%n",
                        nome, exception.getMessage());
            }
        }
    }

    private void iniciarEleicao() {
        Chat.ServerInfo maior = servidores.values().stream()
                .max((a, b) -> Integer.compare(a.getRank(), b.getRank()))
                .orElse(null);
        if (maior == null || maior.getName().equals(nome)) {
            anunciarCoordenador();
        } else {
            coordenador.set(maior.getName());
        }
    }

    private void executarEleicoes() {
        while (executando.get()) {
            try {
                filaEleicao.take();
                Thread.sleep(250L);
                atualizarListaServidores();
                iniciarEleicaoBully();
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception exception) {
                System.err.printf("[AVISO][%s] Eleicao sera tentada novamente: %s%n",
                        nome, exception.getMessage());
            }
        }
    }

    private void iniciarEleicaoBully() {
        boolean servidorMaiorRespondeu = false;
        for (Chat.ServerInfo servidor : servidores.values()) {
            if (servidor.getRank() <= rank.get() || servidor.getEndpoint().isBlank()) {
                continue;
            }
            try {
                Chat.ServerMessage resposta = enviarServidor(servidor.getEndpoint(),
                        novaMensagem(builder -> builder.setElection(
                                Chat.ElectionRequest.newBuilder()
                                        .setServer(nome)
                                        .setRank(rank.get()))));
                if (resposta.hasElectionRes() && resposta.getElectionRes().getStatus()) {
                    servidorMaiorRespondeu = true;
                }
            } catch (Exception exception) {
                System.err.printf("[INFO][%s] Servidor de rank maior %s nao respondeu.%n",
                        nome, servidor.getName());
            }
        }
        if (!servidorMaiorRespondeu) {
            anunciarCoordenador();
        }
    }

    private void anunciarCoordenador() {
        coordenador.set(nome);
        Chat.CoordinatorAnnouncement anuncio;
        synchronized (sincronizadorRelogio) {
            anuncio = Chat.CoordinatorAnnouncement.newBuilder()
                    .setCoordinator(nome)
                    .setRank(rank.get())
                    .setTimestamp(timestampAtual())
                    .setClock(relogio.antesDeEnviar())
                    .build();
        }
        RegistroProtocolo.registrarMensagem("ENVIO", nome, anuncio);
        filaPublicacoes.offer(new MensagemPublicacao(
                TOPICO_SERVIDORES, anuncio.toByteArray()));
        System.out.printf("[ELEICAO][%s] Este servidor assumiu como coordenador, rank %d.%n",
                nome, rank.get());
    }

    private Chat.ReferenceRequest novaRequisicaoReferencia(
            Consumer<Chat.ReferenceRequest.Builder> configurar) {
        Chat.ReferenceRequest.Builder builder = Chat.ReferenceRequest.newBuilder()
                .setId(gerarId())
                .setTimestamp(timestampAtual())
                .setClock(relogio.antesDeEnviar());
        configurar.accept(builder);
        return builder.build();
    }

    private Chat.ReferenceResponse enviarReferencia(Chat.ReferenceRequest requisicao)
            throws Exception {
        try (ZContext contexto = new ZContext()) {
            ZMQ.Socket soquete = contexto.createSocket(SocketType.REQ);
            configurarTimeout(soquete);
            soquete.connect(enderecoReferencia);
            RegistroProtocolo.registrarMensagem("ENVIO", nome, requisicao);
            if (!soquete.send(requisicao.toByteArray(), 0)) {
                throw new IllegalStateException("Falha ao enviar ao No de Referencia");
            }
            byte[] bytes = soquete.recv(0);
            if (bytes == null) {
                throw new IllegalStateException("No de Referencia nao respondeu");
            }
            Chat.ReferenceResponse resposta = Chat.ReferenceResponse.parseFrom(bytes);
            receberRelogio(resposta.getClock());
            RegistroProtocolo.registrarMensagem("RECEBIMENTO", nome, resposta);
            return resposta;
        }
    }

    private Chat.ServerMessage enviarServidor(String endpoint, Chat.ServerMessage requisicao)
            throws Exception {
        try (ZContext contexto = new ZContext()) {
            ZMQ.Socket soquete = contexto.createSocket(SocketType.REQ);
            configurarTimeout(soquete);
            soquete.connect(endpoint);
            RegistroProtocolo.registrarMensagem("ENVIO", nome, requisicao);
            if (!soquete.send(requisicao.toByteArray(), 0)) {
                throw new IllegalStateException("Falha ao enviar requisicao entre servidores");
            }
            byte[] bytes = soquete.recv(0);
            if (bytes == null) {
                throw new IllegalStateException("Servidor remoto nao respondeu: " + endpoint);
            }
            Chat.ServerMessage resposta = Chat.ServerMessage.parseFrom(bytes);
            receberRelogio(resposta.getClock());
            RegistroProtocolo.registrarMensagem("RECEBIMENTO", nome, resposta);
            return resposta;
        }
    }

    private static void configurarTimeout(ZMQ.Socket soquete) {
        soquete.setLinger(0);
        soquete.setSendTimeOut(TIMEOUT_SOQUETE_MS);
        soquete.setReceiveTimeOut(TIMEOUT_SOQUETE_MS);
    }

    private Chat.ServerMessage novaMensagem(
            Consumer<Chat.ServerMessage.Builder> configurar) {
        Chat.ServerMessage.Builder builder = Chat.ServerMessage.newBuilder()
                .setId(gerarId())
                .setTimestamp(timestampAtual())
                .setClock(relogio.antesDeEnviar());
        configurar.accept(builder);
        return builder.build();
    }

    private void receberRelogio(long clock) {
        synchronized (sincronizadorRelogio) {
            relogio.aoReceber(clock);
        }
    }

    private long timestampAtual() {
        return System.currentTimeMillis() + diferencaRelogioFisico.get();
    }

    private void registrarFalhaPersistencia(String operacao, SQLException exception) {
        System.err.printf("[ERRO][%s] Falha SQLite em %s: %s%n",
                nome, operacao, exception.getMessage());
        exception.printStackTrace(System.err);
    }

    private static String gerarId() {
        return UUID.randomUUID().toString();
    }

    private static void iniciarThread(String nomeThread, Runnable tarefa) {
        Thread thread = new Thread(tarefa, nomeThread);
        thread.setDaemon(true);
        thread.start();
    }

    public static void main(String[] args) throws Exception {
        InetAddress enderecoLocal = InetAddress.getLocalHost();
        String nome = System.getenv().getOrDefault("SERVER_ID", enderecoLocal.getHostName());
        String enderecoReferencia = System.getenv().getOrDefault(
                "REFERENCE_ENDPOINT", "tcp://localhost:5559");
        int portaServidores = Integer.parseInt(System.getenv().getOrDefault(
                "SERVER_PEER_PORT", Integer.toString(PORTA_SERVIDORES)));
        String endpointProprio = System.getenv().getOrDefault(
                "SERVER_ENDPOINT", "tcp://" + enderecoLocal.getHostAddress()
                        + ":" + portaServidores);
        Path caminhoBanco = Path.of(System.getenv().getOrDefault("DB_DIR", "/data"))
                .resolve("server_" + nome + ".db");

        try (Persistencia persistencia = new Persistencia(caminhoBanco)) {
            Servidor servidor = new Servidor(
                    nome, endpointProprio, portaServidores, enderecoReferencia, persistencia);
            Runtime.getRuntime().addShutdownHook(new Thread(
                    () -> servidor.executando.set(false), "encerramento-" + nome));
            servidor.iniciar();
        }
    }

    private record MensagemPublicacao(String topico, byte[] conteudo) {
    }
}
