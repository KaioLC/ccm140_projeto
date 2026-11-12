package chat;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Database {
    private Connection conn;

    public Database(String path) {
        try {
            File file = new File(path);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }
            conn = DriverManager.getConnection("jdbc:sqlite:" + path);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS logins (id TEXT PRIMARY KEY, user TEXT, timestamp INT);");
                stmt.execute("CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, name TEXT UNIQUE, timestamp INT);");
                stmt.execute("CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, user TEXT, channel TEXT, content TEXT, timestamp INT);");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public synchronized boolean saveLogin(String id, String user, long ts) {
        String sql = "INSERT OR IGNORE INTO logins VALUES (?, ?, ?);";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, id);
            pstmt.setString(2, user);
            pstmt.setLong(3, ts);
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    public synchronized boolean channelExists(String name) {
        String sql = "SELECT 1 FROM channels WHERE name = ?;";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, name);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            return false;
        }
    }

    public synchronized boolean createChannel(String id, String name, long ts) {
        String sql = "INSERT OR IGNORE INTO channels VALUES (?, ?, ?);";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, id);
            pstmt.setString(2, name);
            pstmt.setLong(3, ts);
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    public synchronized boolean saveMessage(String id, String user, String channel, String content, long ts) {
        String sql = "INSERT OR IGNORE INTO messages VALUES (?, ?, ?, ?, ?);";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, id);
            pstmt.setString(2, user);
            pstmt.setString(3, channel);
            pstmt.setString(4, content);
            pstmt.setLong(5, ts);
            pstmt.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    public synchronized List<String> listChannels() {
        List<String> list = new ArrayList<>();
        String sql = "SELECT name FROM channels;";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                list.add(rs.getString("name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }
}
