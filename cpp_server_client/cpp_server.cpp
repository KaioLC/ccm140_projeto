#include <iostream>
#include <string>
#include <chrono>
#include <fstream>
#include <unordered_set>
#include <random>
#include <sstream>
#include <cstdlib>
#include <zmq.hpp>
#include "chat.pb.h"

class Server {
private:
    zmq::context_t context;
    zmq::socket_t socket;
    std::unordered_set<std::string> users_active;
    std::unordered_set<std::string> channels_active;
    std::string server_id;
    std::string logins_file;
    std::string channels_file;

    int64_t get_timestamp() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

    std::string generate_uuid() {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_int_distribution<> dis(0, 15);
        std::stringstream ss;
        ss << std::hex;
        for (int i = 0; i < 16; i++) ss << dis(gen);
        return ss.str();
    }

    void load_channels() {
        std::ifstream file(channels_file);
        std::string ch;
        while (std::getline(file, ch)) {
            if (!ch.empty()) channels_active.insert(ch);
        }
    }

    void get_login(const chat::LoginRequest& req, int64_t ts, chat::Response& res) {
        std::string user = req.user();
        std::string id = req.id().empty() ? generate_uuid() : req.id();
        chat::LoginResponse* login_res = res.mutable_login();
        login_res->set_id(id);

        if (users_active.insert(user).second) {
            login_res->set_status(true);
            login_res->set_message("Login Successfully.");

            std::ofstream file(logins_file, std::ios::app);
            file << id << ";" << user << ";" << ts << "\n";

            std::cout << "[INFO] New session: " << user << " (ts=" << ts << ")" << std::endl;
        } else {
            login_res->set_status(false);
            login_res->set_message("Login Error: User already exists.");
            std::cout << "[WARN] User already exists: " << user << std::endl;
        }
    }

    void create_channel(const chat::ChannelRequest& req, int64_t ts, chat::Response& res) {
        std::string name = req.name();
        std::string id = req.id().empty() ? generate_uuid() : req.id();
        chat::ChannelResponse* chan_res = res.mutable_channel();
        chan_res->set_id(id);
        chan_res->set_name(name);

        if (channels_active.insert(name).second) {
            chan_res->set_status(true);
            chan_res->set_message("Channel created successfully.");

            std::ofstream file(channels_file, std::ios::app);
            file << name << "\n";

            std::cout << "[INFO] Channel created: " << name << " (ts=" << ts << ")" << std::endl;
        } else {
            chan_res->set_status(false);
            chan_res->set_message("Error: Channel already exists.");
            std::cout << "[WARN] Channel already exists: " << name << std::endl;
        }
    }

    void list_channels(chat::Response& res) {
        chat::ChannelListResponse* list_res = res.mutable_list();
        for (const auto& ch : channels_active) {
            list_res->add_channels(ch);
        }
        std::cout << "[INFO] Channels listed (" << channels_active.size() << " total)" << std::endl;
    }

public:
    Server(const std::string& broker_uri, const std::string& id)
        : context(1),
          socket(context, zmq::socket_type::rep),
          server_id(id),
          logins_file("/data/logins_" + id + ".txt"),
          channels_file("/data/channels_" + id + ".txt") {
        load_channels();
        socket.connect(broker_uri);
        std::cout << "[SERVER " << server_id << "] Connected to " << broker_uri << std::endl;
    }

    void run() {
        while (true) {
            zmq::message_t zmq_msg;
            if (!socket.recv(zmq_msg, zmq::recv_flags::none)) continue;

            chat::Request req;
            req.ParseFromArray(zmq_msg.data(), static_cast<int>(zmq_msg.size()));

            std::cout << "[RECV] id=" << req.id() << " ts=" << req.timestamp() << std::endl;

            chat::Response res;
            res.set_id(generate_uuid());
            res.set_timestamp(get_timestamp());

            switch (req.payload_case()) {
                case chat::Request::kLogin:
                    get_login(req.login(), req.timestamp(), res);
                    break;
                case chat::Request::kChannel:
                    create_channel(req.channel(), req.timestamp(), res);
                    break;
                case chat::Request::kList:
                    list_channels(res);
                    break;
                default:
                    break;
            }

            std::string out;
            res.SerializeToString(&out);
            socket.send(zmq::buffer(out), zmq::send_flags::none);
            std::cout << "[SEND] id=" << res.id() << " ts=" << res.timestamp() << std::endl;
        }
    }
};

int main() {
    const char* host = std::getenv("HOSTNAME");
    std::string server_id = host ? std::string(host) : "local";

    Server server("tcp://broker:5556", server_id);
    server.run();
    return 0;
}
