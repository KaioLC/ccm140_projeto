#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <cstdlib>
#include <sqlite3.h>
#include <zmq.hpp>
#include "chat.pb.h"

class Database {
private:
    sqlite3* db;

public:
    Database(const std::string& path) {
        sqlite3_open(path.c_str(), &db);
        sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS logins (id TEXT PRIMARY KEY, user TEXT, timestamp INT);", nullptr, nullptr, nullptr);
        sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS channels (id TEXT PRIMARY KEY, name TEXT UNIQUE, timestamp INT);", nullptr, nullptr, nullptr);
        sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, user TEXT, channel TEXT, content TEXT, timestamp INT);", nullptr, nullptr, nullptr);
    }

    ~Database() {
        sqlite3_close(db);
    }

    bool save_login(const std::string& id, const std::string& user, int64_t ts) {
        std::string sql = "INSERT INTO logins VALUES ('" + id + "', '" + user + "', " + std::to_string(ts) + ");";
        return sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) == SQLITE_OK;
    }

    bool create_channel(const std::string& id, const std::string& name, int64_t ts) {
        std::string sql = "INSERT INTO channels VALUES ('" + id + "', '" + name + "', " + std::to_string(ts) + ");";
        return sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) == SQLITE_OK;
    }

    bool save_message(const std::string& id, const std::string& user, const std::string& channel, const std::string& content, int64_t ts) {
        std::string sql = "INSERT INTO messages VALUES ('" + id + "', '" + user + "', '" + channel + "', '" + content + "', " + std::to_string(ts) + ");";
        return sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr) == SQLITE_OK;
    }

    std::vector<std::string> list_channels() {
        std::vector<std::string> list;
        sqlite3_stmt* stmt;
        if (sqlite3_prepare_v2(db, "SELECT name FROM channels;", -1, &stmt, nullptr) == SQLITE_OK) {
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                list.push_back((const char*)sqlite3_column_text(stmt, 0));
            }
        }
        sqlite3_finalize(stmt);
        return list;
    }
};

class Server {
private:
    zmq::context_t context;
    zmq::socket_t rep_socket;
    zmq::socket_t pub_socket;
    Database db;

    int64_t get_timestamp() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

    std::string gen_id() {
        return std::to_string(get_timestamp()) + "_" + std::to_string(std::rand() % 10000);
    }

    void get_login(const chat::LoginRequest& req, int64_t ts, chat::Response& res) {
        std::string user = req.user();
        auto* login_res = res.mutable_login();
        login_res->set_id(req.id());

        if (db.save_login(req.id(), user, ts)) {
            login_res->set_status(true);
            login_res->set_message("Login Successfully.");
            std::cout << "[INFO] New session: " << user << std::endl;
        } else {
            login_res->set_status(false);
            login_res->set_message("Login Error.");
            std::cout << "[WARN] Login failed: " << user << std::endl;
        }
    }

    void create_channel(const chat::ChannelRequest& req, int64_t ts, chat::Response& res) {
        std::string name = req.name();
        auto* chan_res = res.mutable_channel();
        chan_res->set_id(req.id());
        chan_res->set_name(name);

        if (db.create_channel(req.id(), name, ts)) {
            chan_res->set_status(true);
            chan_res->set_message("Channel created.");
            std::cout << "[INFO] Channel created: " << name << std::endl;
        } else {
            chan_res->set_status(false);
            chan_res->set_message("Channel already exists.");
            std::cout << "[WARN] Channel already exists: " << name << std::endl;
        }
    }

    void list_channels(chat::Response& res) {
        auto* list_res = res.mutable_list();
        for (const auto& ch : db.list_channels()) {
            list_res->add_channels(ch);
        }
    }

    void publish_message(const chat::PublishRequest& req, int64_t ts, chat::Response& res) {
        auto* pub_res = res.mutable_publish();
        pub_res->set_id(req.id());

        if (db.save_message(req.id(), req.user(), req.channel(), req.content(), ts)) {
            chat::Publication pub;
            pub.set_id(req.id());
            pub.set_user(req.user());
            pub.set_channel(req.channel());
            pub.set_content(req.content());
            pub.set_timestamp(get_timestamp());

            std::string out;
            pub.SerializeToString(&out);
            pub_socket.send(zmq::buffer(req.channel()), zmq::send_flags::sndmore);
            pub_socket.send(zmq::buffer(out), zmq::send_flags::none);

            pub_res->set_status(true);
            pub_res->set_message("Published OK.");
            std::cout << "[PUB] canal=" << req.channel() << " user=" << req.user()
                      << " msg=\"" << req.content() << "\" ts=" << pub.timestamp() << std::endl;
        } else {
            pub_res->set_status(false);
            pub_res->set_message("Publish Error.");
        }
    }

public:
    Server(const std::string& broker_uri, const std::string& proxy_uri, const std::string& db_path)
        : context(1),
          rep_socket(context, zmq::socket_type::rep),
          pub_socket(context, zmq::socket_type::pub),
          db(db_path) {
        rep_socket.connect(broker_uri);
        pub_socket.connect(proxy_uri);
    }

    void run() {
        while (true) {
            zmq::message_t msg;
            rep_socket.recv(msg, zmq::recv_flags::none);

            chat::Request req;
            req.ParseFromString(msg.to_string());
            std::cout << "[RECV] id=" << req.id() << " ts=" << req.timestamp() << std::endl;

            chat::Response res;
            res.set_id(gen_id());
            res.set_timestamp(get_timestamp());

            if (req.has_login()) get_login(req.login(), req.timestamp(), res);
            else if (req.has_channel()) create_channel(req.channel(), req.timestamp(), res);
            else if (req.has_list()) list_channels(res);
            else if (req.has_publish()) publish_message(req.publish(), req.timestamp(), res);

            std::string out;
            res.SerializeToString(&out);
            rep_socket.send(zmq::buffer(out), zmq::send_flags::none);
            std::cout << "[SEND] id=" << res.id() << " ts=" << res.timestamp() << std::endl;
        }
    }
};

int main() {
    const char* host = std::getenv("HOSTNAME");
    std::string id = host ? host : "local";

    Server server("tcp://broker:5556", "tcp://proxy:5557", "/data/server_" + id + ".db");
    server.run();
    return 0;
}
