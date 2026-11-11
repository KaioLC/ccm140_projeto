package chat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public final class Persistencia implements AutoCloseable {
    private final Connection conexao;

    public Persistencia(Path caminhoBanco) throws SQLException, IOException {
        Path caminhoAbsoluto = caminhoBanco.toAbsolutePath();
        Path pastaPai = caminhoAbsoluto.getParent();
        if (pastaPai != null) {
            Files.createDirectories(pastaPai);
        }

        conexao = DriverManager.getConnection("jdbc:sqlite:" + caminhoAbsoluto);
        inicializarEsquema();
    }

    private void inicializarEsquema() throws SQLException {
        try (Statement instrucao = conexao.createStatement()) {
            instrucao.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS logins ("
                            + "id TEXT PRIMARY KEY, user TEXT, timestamp INT, clock INT NOT NULL DEFAULT 0)");
            instrucao.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS channels ("
                            + "id TEXT PRIMARY KEY, name TEXT UNIQUE, timestamp INT, clock INT NOT NULL DEFAULT 0)");
            instrucao.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS messages ("
                            + "id TEXT PRIMARY KEY, user TEXT, channel TEXT, "
                            + "content TEXT, timestamp INT, clock INT NOT NULL DEFAULT 0)");
        }
        garantirColunaClock("logins");
        garantirColunaClock("channels");
        garantirColunaClock("messages");
    }

    private void garantirColunaClock(String tabela) throws SQLException {
        boolean existe = false;
        try (Statement instrucao = conexao.createStatement();
             ResultSet colunas = instrucao.executeQuery("PRAGMA table_info(" + tabela + ")")) {
            while (colunas.next()) {
                if ("clock".equalsIgnoreCase(colunas.getString("name"))) {
                    existe = true;
                    break;
                }
            }
        }
        if (!existe) {
            try (Statement instrucao = conexao.createStatement()) {
                instrucao.executeUpdate("ALTER TABLE " + tabela
                        + " ADD COLUMN clock INT NOT NULL DEFAULT 0");
                instrucao.executeUpdate("UPDATE " + tabela + " SET clock = 1 WHERE clock = 0");
            }
        }
    }

    public synchronized boolean aplicarEvento(Chat.ReplicationEvent evento) throws SQLException {
        return switch (evento.getType()) {
            case REP_LOGIN -> inserirLogin(evento);
            case REP_CHANNEL -> inserirCanalDeterministico(evento);
            case REP_MESSAGE -> inserirMensagem(evento);
            case UNRECOGNIZED -> throw new SQLException(
                    "Tipo de evento de replicacao desconhecido: " + evento.getTypeValue());
        };
    }

    private boolean inserirLogin(Chat.ReplicationEvent evento) throws SQLException {
        String sql = "INSERT OR IGNORE INTO logins (id, user, timestamp, clock) "
                + "VALUES (?, ?, ?, ?)";
        try (PreparedStatement instrucao = conexao.prepareStatement(sql)) {
            instrucao.setString(1, evento.getId());
            instrucao.setString(2, evento.getUser());
            instrucao.setLong(3, evento.getTimestamp());
            instrucao.setLong(4, evento.getClock());
            return instrucao.executeUpdate() == 1;
        }
    }

    private boolean inserirCanalDeterministico(Chat.ReplicationEvent evento) throws SQLException {
        String sql = "INSERT OR IGNORE INTO channels (id, name, timestamp, clock) "
                + "VALUES (?, ?, ?, ?)";
        try (PreparedStatement instrucao = conexao.prepareStatement(sql)) {
            instrucao.setString(1, evento.getId());
            instrucao.setString(2, evento.getName());
            instrucao.setLong(3, evento.getTimestamp());
            instrucao.setLong(4, evento.getClock());
            if (instrucao.executeUpdate() == 1) {
                return true;
            }
        }

        String consultar = "SELECT id FROM channels WHERE name = ?";
        try (PreparedStatement instrucao = conexao.prepareStatement(consultar)) {
            instrucao.setString(1, evento.getName());
            try (ResultSet resultado = instrucao.executeQuery()) {
                if (!resultado.next() || evento.getId().compareTo(resultado.getString("id")) >= 0) {
                    return false;
                }
            }
        }

        String atualizar = "UPDATE channels SET id = ?, timestamp = ?, clock = ? WHERE name = ?";
        try (PreparedStatement instrucao = conexao.prepareStatement(atualizar)) {
            instrucao.setString(1, evento.getId());
            instrucao.setLong(2, evento.getTimestamp());
            instrucao.setLong(3, evento.getClock());
            instrucao.setString(4, evento.getName());
            return instrucao.executeUpdate() == 1;
        }
    }

    private boolean inserirMensagem(Chat.ReplicationEvent evento) throws SQLException {
        String sql = "INSERT OR IGNORE INTO messages "
                + "(id, user, channel, content, timestamp, clock) VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement instrucao = conexao.prepareStatement(sql)) {
            instrucao.setString(1, evento.getId());
            instrucao.setString(2, evento.getUser());
            instrucao.setString(3, evento.getChannel());
            instrucao.setString(4, evento.getContent());
            instrucao.setLong(5, evento.getTimestamp());
            instrucao.setLong(6, evento.getClock());
            return instrucao.executeUpdate() == 1;
        }
    }

    public synchronized List<String> listarCanais() throws SQLException {
        List<String> canais = new ArrayList<>();
        try (Statement instrucao = conexao.createStatement();
             ResultSet resultados = instrucao.executeQuery(
                     "SELECT name FROM channels ORDER BY name")) {
            while (resultados.next()) {
                canais.add(resultados.getString("name"));
            }
        }
        return canais;
    }

    public synchronized List<Chat.ReplicationEvent> listarEventos(String remetente)
            throws SQLException {
        List<Chat.ReplicationEvent> eventos = new ArrayList<>();
        try (Statement instrucao = conexao.createStatement();
             ResultSet resultados = instrucao.executeQuery(
                     "SELECT id, user, timestamp, clock FROM logins ORDER BY id")) {
            while (resultados.next()) {
                eventos.add(Chat.ReplicationEvent.newBuilder()
                        .setType(Chat.ReplicationType.REP_LOGIN)
                        .setSender(remetente)
                        .setId(resultados.getString("id"))
                        .setUser(resultados.getString("user"))
                        .setTimestamp(resultados.getLong("timestamp"))
                        .setClock(resultados.getLong("clock"))
                        .build());
            }
        }
        try (Statement instrucao = conexao.createStatement();
             ResultSet resultados = instrucao.executeQuery(
                     "SELECT id, name, timestamp, clock FROM channels ORDER BY id")) {
            while (resultados.next()) {
                eventos.add(Chat.ReplicationEvent.newBuilder()
                        .setType(Chat.ReplicationType.REP_CHANNEL)
                        .setSender(remetente)
                        .setId(resultados.getString("id"))
                        .setName(resultados.getString("name"))
                        .setTimestamp(resultados.getLong("timestamp"))
                        .setClock(resultados.getLong("clock"))
                        .build());
            }
        }
        try (Statement instrucao = conexao.createStatement();
             ResultSet resultados = instrucao.executeQuery(
                     "SELECT id, user, channel, content, timestamp, clock FROM messages ORDER BY id")) {
            while (resultados.next()) {
                eventos.add(Chat.ReplicationEvent.newBuilder()
                        .setType(Chat.ReplicationType.REP_MESSAGE)
                        .setSender(remetente)
                        .setId(resultados.getString("id"))
                        .setUser(resultados.getString("user"))
                        .setChannel(resultados.getString("channel"))
                        .setContent(resultados.getString("content"))
                        .setTimestamp(resultados.getLong("timestamp"))
                        .setClock(resultados.getLong("clock"))
                        .build());
            }
        }
        return eventos;
    }

    @Override
    public synchronized void close() throws SQLException {
        conexao.close();
    }
}
