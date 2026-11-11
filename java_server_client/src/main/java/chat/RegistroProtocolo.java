package chat;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.InvalidProtocolBufferException;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;

final class RegistroProtocolo {
    private static final DateTimeFormatter FORMATO_TIMESTAMP =
            DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneOffset.UTC);

    private RegistroProtocolo() {
    }

    static void registrarMensagem(String direcao, String participante, MessageOrBuilder mensagem) {
        if (mensagem instanceof Chat.Request requisicao) {
            imprimir(direcao, participante, requisicao.getId(), requisicao.getTimestamp(),
                    requisicao.getClock(), tipoPayload(requisicao.getPayloadCase().name()),
                    obterPayload(requisicao));
        } else if (mensagem instanceof Chat.Response resposta) {
            imprimir(direcao, participante, resposta.getId(), resposta.getTimestamp(),
                    resposta.getClock(), tipoPayload(resposta.getPayloadCase().name()),
                    obterPayload(resposta));
        } else if (mensagem instanceof Chat.ReferenceRequest requisicao) {
            imprimir(direcao, participante, requisicao.getId(), requisicao.getTimestamp(),
                    requisicao.getClock(), tipoPayload(requisicao.getPayloadCase().name()),
                    obterPayload(requisicao));
        } else if (mensagem instanceof Chat.ReferenceResponse resposta) {
            imprimir(direcao, participante, resposta.getId(), resposta.getTimestamp(),
                    resposta.getClock(), tipoPayload(resposta.getPayloadCase().name()),
                    obterPayload(resposta));
        } else if (mensagem instanceof Chat.ServerMessage mensagemServidor) {
            imprimir(direcao, participante, mensagemServidor.getId(),
                    mensagemServidor.getTimestamp(), mensagemServidor.getClock(),
                    tipoPayload(mensagemServidor.getPayloadCase().name()),
                    obterPayload(mensagemServidor));
        } else if (mensagem instanceof Chat.Publication publicacao) {
            imprimir(direcao, participante, publicacao.getId(), publicacao.getTimestamp(),
                    publicacao.getClock(), "PUBLICACAO", publicacao);
        } else if (mensagem instanceof Chat.ReplicationEvent evento) {
            imprimir(direcao, participante, evento.getId(), evento.getTimestamp(),
                    evento.getClock(), "REPLICACAO_" + evento.getType(), evento);
        } else if (mensagem instanceof Chat.CoordinatorAnnouncement anuncio) {
            imprimir(direcao, participante, anuncio.getCoordinator(), anuncio.getTimestamp(),
                    anuncio.getClock(), "ANUNCIO_COORDENADOR", anuncio);
        } else {
            System.out.printf("[%s][%s]%n%s%n", direcao, participante, mensagem);
        }
    }

    static void registrarRequisicaoBinaria(String direcao, String participante, byte[] bytes) {
        try {
            registrarMensagem(direcao, participante, Chat.Request.parseFrom(bytes));
        } catch (InvalidProtocolBufferException exception) {
            registrarQuadroBinario(direcao, participante, bytes);
        }
    }

    static void registrarRespostaBinaria(String direcao, String participante, byte[] bytes) {
        try {
            registrarMensagem(direcao, participante, Chat.Response.parseFrom(bytes));
        } catch (InvalidProtocolBufferException exception) {
            registrarQuadroBinario(direcao, participante, bytes);
        }
    }

    static void registrarPublicacaoBinaria(String direcao, String participante, byte[] bytes) {
        try {
            registrarMensagem(direcao, participante, Chat.Publication.parseFrom(bytes));
        } catch (InvalidProtocolBufferException exception) {
            registrarQuadroBinario(direcao, participante, bytes);
        }
    }

    static void registrarEventoReplicacaoBinario(
            String direcao, String participante, byte[] bytes) {
        try {
            registrarMensagem(direcao, participante, Chat.ReplicationEvent.parseFrom(bytes));
        } catch (InvalidProtocolBufferException exception) {
            registrarQuadroBinario(direcao, participante, bytes);
        }
    }

    static void registrarQuadroBinario(String direcao, String participante, byte[] bytes) {
        System.out.printf("[%s][%s] quadro binario (%d bytes): %s%n",
                direcao, participante, bytes.length, HexFormat.of().formatHex(bytes));
    }

    private static void imprimir(String direcao, String participante, String id,
                                 long registroTempo, long lamport, String tipo, Message conteudo) {
        String timestampLegivel = FORMATO_TIMESTAMP.format(Instant.ofEpochMilli(registroTempo));
        System.out.printf("[%s][%s] tipo=%s id=%s timestamp=%s (epochMs=%d) clock(Lamport)=%d%n",
                direcao, participante, tipo, id, timestampLegivel, registroTempo, lamport);
        if (conteudo == null) {
            System.out.println("  conteudo: {}");
        } else {
            String texto = conteudo.toString().stripTrailing();
            if (texto.isEmpty()) {
                System.out.println("  conteudo: {}");
            } else {
                for (String linha : texto.split("\\R")) {
                    System.out.println("  " + linha);
                }
            }
        }
    }

    private static String tipoPayload(String tipoOriginal) {
        return switch (tipoOriginal) {
            case "LOGIN" -> "LOGIN";
            case "CHANNEL" -> "CANAL";
            case "LIST" -> "LISTAGEM";
            case "PUBLISH" -> "PUBLICACAO";
            case "REG" -> "CADASTRO_SERVIDOR";
            case "HEARTBEAT" -> "HEARTBEAT";
            case "CLOCK_SYNC" -> "SINCRONIZACAO_RELOGIO";
            case "CLOCK_RES" -> "RESPOSTA_SINCRONIZACAO";
            case "ELECTION" -> "ELEICAO";
            case "ELECTION_RES" -> "RESPOSTA_ELEICAO";
            case "REPLICATION_SYNC" -> "SINCRONIZACAO_REPLICACAO";
            case "REPLICATION_SNAPSHOT" -> "SNAPSHOT_REPLICACAO";
            default -> "SEM_CONTEUDO";
        };
    }

    private static Message obterPayload(Chat.Request requisicao) {
        return switch (requisicao.getPayloadCase()) {
            case LOGIN -> requisicao.getLogin();
            case CHANNEL -> requisicao.getChannel();
            case LIST -> requisicao.getList();
            case PUBLISH -> requisicao.getPublish();
            case PAYLOAD_NOT_SET -> null;
        };
    }

    private static Message obterPayload(Chat.Response resposta) {
        return switch (resposta.getPayloadCase()) {
            case LOGIN -> resposta.getLogin();
            case CHANNEL -> resposta.getChannel();
            case LIST -> resposta.getList();
            case PUBLISH -> resposta.getPublish();
            case PAYLOAD_NOT_SET -> null;
        };
    }

    private static Message obterPayload(Chat.ReferenceRequest requisicao) {
        return switch (requisicao.getPayloadCase()) {
            case REG -> requisicao.getReg();
            case LIST -> requisicao.getList();
            case HEARTBEAT -> requisicao.getHeartbeat();
            case PAYLOAD_NOT_SET -> null;
        };
    }

    private static Message obterPayload(Chat.ReferenceResponse resposta) {
        return switch (resposta.getPayloadCase()) {
            case REG -> resposta.getReg();
            case LIST -> resposta.getList();
            case HEARTBEAT -> resposta.getHeartbeat();
            case PAYLOAD_NOT_SET -> null;
        };
    }

    private static Message obterPayload(Chat.ServerMessage mensagem) {
        return switch (mensagem.getPayloadCase()) {
            case CLOCK_SYNC -> mensagem.getClockSync();
            case CLOCK_RES -> mensagem.getClockRes();
            case ELECTION -> mensagem.getElection();
            case ELECTION_RES -> mensagem.getElectionRes();
            case REPLICATION_SYNC -> mensagem.getReplicationSync();
            case REPLICATION_SNAPSHOT -> mensagem.getReplicationSnapshot();
            case PAYLOAD_NOT_SET -> null;
        };
    }
}
