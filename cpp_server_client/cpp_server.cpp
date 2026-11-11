#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <cstdlib>
#include <algorithm>
#include <thread>
#include <atomic>
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
    std::string server_name;
    std::string endpoint;
    std::string proxy_sub_uri;
    std::string coordinator;
    int rank;
    int64_t clock;
    int64_t clock_offset;
    int req_count;
    std::atomic<bool> running;

    zmq::context_t context;
    zmq::socket_t rep_socket;
    zmq::socket_t pub_socket;
    zmq::socket_t ref_socket;
    zmq::socket_t peer_rep_socket;
    Database db;

    int64_t get_system_time() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

    int64_t get_timestamp() {
        return get_system_time() + clock_offset;
    }

    std::string gen_id() {
        return std::to_string(get_timestamp()) + "_" + std::to_string(std::rand() % 10000);
    }

    void register_in_reference() {
        chat::ReferenceRequest req;
        clock++;
        req.set_id(gen_id());
        req.set_timestamp(get_timestamp());
        req.set_clock(clock);
        auto* reg = req.mutable_reg();
        reg->set_name(server_name);
        reg->set_endpoint(endpoint);

        std::string out;
        req.SerializeToString(&out);
        ref_socket.send(zmq::buffer(out), zmq::send_flags::none);

        zmq::message_t reply;
        ref_socket.recv(reply, zmq::recv_flags::none);

        chat::ReferenceResponse res;
        res.ParseFromString(reply.to_string());
        clock = std::max(clock, res.clock());
        rank = res.reg().rank();

        std::cout << "[REF] Registrado: nome=" << server_name
                  << " rank=" << rank << " endpoint=" << endpoint << std::endl;
    }

    std::vector<chat::ServerInfo> fetch_server_list() {
        chat::ReferenceRequest req;
        clock++;
        req.set_id(gen_id());
        req.set_timestamp(get_timestamp());
        req.set_clock(clock);
        req.mutable_list();

        std::string out;
        req.SerializeToString(&out);
        ref_socket.send(zmq::buffer(out), zmq::send_flags::none);

        zmq::message_t reply;
        ref_socket.recv(reply, zmq::recv_flags::none);

        chat::ReferenceResponse res;
        res.ParseFromString(reply.to_string());
        clock = std::max(clock, res.clock());

        std::vector<chat::ServerInfo> list;
        for (const auto& s : res.list().servers()) {
            list.push_back(s);
        }
        return list;
    }

    void announce_coordinator() {
        coordinator = server_name;
        chat::CoordinatorAnnouncement ann;
        ann.set_coordinator(server_name);
        ann.set_rank(rank);
        ann.set_timestamp(get_timestamp());
        clock++;
        ann.set_clock(clock);

        std::string out;
        ann.SerializeToString(&out);
        pub_socket.send(zmq::buffer(std::string("servers")), zmq::send_flags::sndmore);
        pub_socket.send(zmq::buffer(out), zmq::send_flags::none);

        std::cout << "[ELECTION] Eleito coordenador: " << server_name << " (rank " << rank << ")" << std::endl;
    }

    void start_election() {
        std::cout << "[ELECTION] Iniciando eleicao por bullying..." << std::endl;
        auto servers = fetch_server_list();
        bool higher_alive = false;

        for (const auto& s : servers) {
            if (s.rank() > rank && s.name() != server_name && !s.endpoint().empty()) {
                try {
                    zmq::socket_t req(context, zmq::socket_type::req);
                    req.set(zmq::sockopt::rcvtimeo, 1000);
                    req.set(zmq::sockopt::sndtimeo, 1000);
                    req.connect(s.endpoint());

                    chat::ServerMessage msg;
                    clock++;
                    msg.set_id(gen_id());
                    msg.set_timestamp(get_timestamp());
                    msg.set_clock(clock);
                    auto* el = msg.mutable_election();
                    el->set_server(server_name);
                    el->set_rank(rank);

                    std::string out;
                    msg.SerializeToString(&out);
                    req.send(zmq::buffer(out), zmq::send_flags::none);

                    zmq::message_t reply;
                    if (req.recv(reply, zmq::recv_flags::none)) {
                        chat::ServerMessage rep;
                        rep.ParseFromString(reply.to_string());
                        clock = std::max(clock, rep.clock());
                        if (rep.has_election_res() && rep.election_res().status()) {
                            higher_alive = true;
                            std::cout << "[ELECTION] Servidor superior " << s.name() << " respondeu OK" << std::endl;
                        }
                    }
                } catch (...) {}
            }
        }

        if (!higher_alive) {
            announce_coordinator();
        }
    }

    void sync_clock_with_coordinator() {
        if (coordinator == server_name) return;

        if (coordinator.empty()) {
            start_election();
            return;
        }

        std::string coord_endpoint;
        auto servers = fetch_server_list();
        for (const auto& s : servers) {
            if (s.name() == coordinator) {
                coord_endpoint = s.endpoint();
                break;
            }
        }

        if (coord_endpoint.empty()) {
            start_election();
            return;
        }

        try {
            zmq::socket_t req(context, zmq::socket_type::req);
            req.set(zmq::sockopt::rcvtimeo, 1500);
            req.set(zmq::sockopt::sndtimeo, 1500);
            req.connect(coord_endpoint);

            chat::ServerMessage msg;
            clock++;
            msg.set_id(gen_id());
            msg.set_timestamp(get_timestamp());
            msg.set_clock(clock);
            msg.mutable_clock_sync()->set_server(server_name);

            std::string out;
            msg.SerializeToString(&out);
            req.send(zmq::buffer(out), zmq::send_flags::none);

            zmq::message_t reply;
            if (req.recv(reply, zmq::recv_flags::none)) {
                chat::ServerMessage rep;
                rep.ParseFromString(reply.to_string());
                clock = std::max(clock, rep.clock());
                if (rep.has_clock_res()) {
                    int64_t coord_time = rep.clock_res().physical_time();
                    int64_t sys_now = get_system_time();
                    clock_offset = coord_time - sys_now;
                    std::cout << "[BERKELEY] Sincronizado com coordenador " << coordinator
                              << " | Offset: " << clock_offset << "ms | Hora: " << get_timestamp() << std::endl;
                    return;
                }
            }
        } catch (...) {}

        std::cout << "[WARN] Coordenador " << coordinator << " nao respondeu ao relogio." << std::endl;
        start_election();
    }

    void send_heartbeat() {
        chat::ReferenceRequest req;
        clock++;
        req.set_id(gen_id());
        req.set_timestamp(get_timestamp());
        req.set_clock(clock);
        req.mutable_heartbeat()->set_name(server_name);

        std::string out;
        req.SerializeToString(&out);
        ref_socket.send(zmq::buffer(out), zmq::send_flags::none);

        zmq::message_t reply;
        ref_socket.recv(reply, zmq::recv_flags::none);

        chat::ReferenceResponse res;
        res.ParseFromString(reply.to_string());
        clock = std::max(clock, res.clock());

        std::cout << "[HEARTBEAT] Enviado (msg #" << req_count << ") | Clock: " << clock << std::endl;
    }

    void listen_peer() {
        while (running) {
            zmq::message_t msg;
            if (!peer_rep_socket.recv(msg, zmq::recv_flags::none)) continue;

            chat::ServerMessage req;
            req.ParseFromString(msg.to_string());
            clock = std::max(clock, req.clock());

            chat::ServerMessage rep;
            rep.set_id(gen_id());
            rep.set_timestamp(get_timestamp());

            if (req.has_clock_sync()) {
                auto* sync = rep.mutable_clock_res();
                sync->set_server(server_name);
                sync->set_physical_time(get_timestamp());
            } else if (req.has_election()) {
                auto* elec = rep.mutable_election_res();
                elec->set_server(server_name);
                elec->set_status(true);
            }

            clock++;
            rep.set_clock(clock);
            std::string out;
            rep.SerializeToString(&out);
            peer_rep_socket.send(zmq::buffer(out), zmq::send_flags::none);
        }
    }

    void listen_servers_sub() {
        zmq::socket_t sub(context, zmq::socket_type::sub);
        sub.connect(proxy_sub_uri);
        sub.set(zmq::sockopt::subscribe, "servers");

        while (running) {
            zmq::message_t topic;
            zmq::message_t data;
            if (!sub.recv(topic, zmq::recv_flags::none)) continue;
            if (!sub.recv(data, zmq::recv_flags::none)) continue;

            chat::CoordinatorAnnouncement ann;
            ann.ParseFromString(data.to_string());
            clock = std::max(clock, ann.clock());
            coordinator = ann.coordinator();

            std::cout << "[ELECTION] Coordenador atualizado via 'servers': " << coordinator
                      << " (rank " << ann.rank() << ")" << std::endl;
        }
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
            clock++;
            pub.set_clock(clock);

            std::string out;
            pub.SerializeToString(&out);
            pub_socket.send(zmq::buffer(req.channel()), zmq::send_flags::sndmore);
            pub_socket.send(zmq::buffer(out), zmq::send_flags::none);

            pub_res->set_status(true);
            pub_res->set_message("Published OK.");
            std::cout << "[PUB] canal=" << req.channel() << " user=" << req.user()
                      << " msg=\"" << req.content() << "\" ts=" << pub.timestamp()
                      << " clock=" << pub.clock() << std::endl;
        } else {
            pub_res->set_status(false);
            pub_res->set_message("Publish Error.");
        }
    }

public:
    Server(const std::string& broker_uri, const std::string& proxy_pub_uri, const std::string& proxy_sub,
           const std::string& ref_uri, const std::string& name, const std::string& host_id, const std::string& db_path)
        : server_name(name),
          endpoint("tcp://" + host_id + ":6000"),
          proxy_sub_uri(proxy_sub),
          coordinator(""),
          rank(0),
          clock(0),
          clock_offset(0),
          req_count(0),
          running(true),
          context(1),
          rep_socket(context, zmq::socket_type::rep),
          pub_socket(context, zmq::socket_type::pub),
          ref_socket(context, zmq::socket_type::req),
          peer_rep_socket(context, zmq::socket_type::rep),
          db(db_path) {
        rep_socket.connect(broker_uri);
        pub_socket.connect(proxy_pub_uri);
        ref_socket.connect(ref_uri);
        peer_rep_socket.bind("tcp://0.0.0.0:6000");
    }

    ~Server() {
        running = false;
    }

    void run() {
        register_in_reference();

        std::thread peer_t(&Server::listen_peer, this);
        peer_t.detach();

        std::thread sub_t(&Server::listen_servers_sub, this);
        sub_t.detach();

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        start_election();

        while (true) {
            zmq::message_t msg;
            rep_socket.recv(msg, zmq::recv_flags::none);

            chat::Request req;
            req.ParseFromString(msg.to_string());
            clock = std::max(clock, req.clock());
            req_count++;

            std::cout << "[RECV] id=" << req.id() << " ts=" << req.timestamp()
                      << " clock=" << req.clock() << std::endl;

            if (req_count % 10 == 0) {
                send_heartbeat();
            }

            if (req_count % 15 == 0) {
                sync_clock_with_coordinator();
            }

            chat::Response res;
            res.set_id(gen_id());
            res.set_timestamp(get_timestamp());

            if (req.has_login()) get_login(req.login(), req.timestamp(), res);
            else if (req.has_channel()) create_channel(req.channel(), req.timestamp(), res);
            else if (req.has_list()) list_channels(res);
            else if (req.has_publish()) publish_message(req.publish(), req.timestamp(), res);

            clock++;
            res.set_clock(clock);

            std::string out;
            res.SerializeToString(&out);
            rep_socket.send(zmq::buffer(out), zmq::send_flags::none);
            std::cout << "[SEND] id=" << res.id() << " ts=" << res.timestamp()
                      << " clock=" << res.clock() << std::endl;
        }
    }
};

int main() {
    const char* host = std::getenv("HOSTNAME");
    std::string id = host ? host : "local";
    std::srand(std::time(nullptr));

    Server server("tcp://broker:5556", "tcp://proxy:5557", "tcp://proxy:5558", "tcp://reference:5559",
                  "srv_cpp_" + id, id, "/data/server_" + id + ".db");
    server.run();
    return 0;
}
