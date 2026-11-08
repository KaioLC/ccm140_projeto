#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <random>
#include <sstream>
#include <cstdlib>
#include <zmq.hpp>
#include "chat.pb.h"

class Client {
private:
    std::string username;
    std::string user_id;
    zmq::context_t context;
    zmq::socket_t socket;

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

    chat::Response send_request(chat::Request& req) {
        req.set_id(generate_uuid());
        req.set_timestamp(get_timestamp());

        std::string out;
        req.SerializeToString(&out);
        std::cout << "[SEND] user=" << username << " id=" << req.id()
                  << " ts=" << req.timestamp() << std::endl;

        socket.send(zmq::buffer(out), zmq::send_flags::none);

        zmq::message_t reply;
        chat::Response res;
        if (socket.recv(reply, zmq::recv_flags::none)) {
            res.ParseFromArray(reply.data(), static_cast<int>(reply.size()));
            std::cout << "[RECV] user=" << username << " id=" << res.id()
                      << " ts=" << res.timestamp() << std::endl;
        }
        return res;
    }

public:
    Client(const std::string& broker_uri, const std::string& bot_name)
        : username(bot_name),
          user_id(generate_uuid()),
          context(1),
          socket(context, zmq::socket_type::req) {
        socket.connect(broker_uri);
        std::cout << "[CLIENT " << username << "] Connected to " << broker_uri << std::endl;
    }

    void login() {
        while (true) {
            chat::Request req;
            chat::LoginRequest* login_req = req.mutable_login();
            login_req->set_id(user_id);
            login_req->set_user(username);

            chat::Response res = send_request(req);
            if (res.has_login() && res.login().status()) {
                std::cout << "[INFO] Login OK: " << res.login().message() << std::endl;
                break;
            }
            std::cout << "[WARN] Login failed: " << res.login().message() << " Retrying..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }

    std::vector<std::string> list_channels() {
        chat::Request req;
        req.mutable_list();
        chat::Response res = send_request(req);

        std::vector<std::string> channels;
        if (res.has_list()) {
            std::cout << "[INFO] Channels (" << res.list().channels_size() << "): ";
            for (const auto& ch : res.list().channels()) {
                std::cout << ch << " ";
                channels.push_back(ch);
            }
            std::cout << std::endl;
        }
        return channels;
    }

    void create_channel(const std::string& name) {
        chat::Request req;
        chat::ChannelRequest* chan_req = req.mutable_channel();
        chan_req->set_id(generate_uuid());
        chan_req->set_name(name);

        chat::Response res = send_request(req);
        if (res.has_channel()) {
            std::cout << "[INFO] Create channel '" << res.channel().name() << "': "
                      << (res.channel().status() ? "OK" : "ERROR") << " - "
                      << res.channel().message() << std::endl;
        }
    }

    void run() {
        login();

        int count = 1;
        while (true) {
            auto channels = list_channels();
            if (channels.size() < 5) {
                create_channel("canal_" + username + "_" + std::to_string(count++));
            }
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
};

int main() {
    const char* host = std::getenv("HOSTNAME");
    std::string client_id = host ? std::string(host) : "local";

    Client client("tcp://broker:5555", "bot_cpp_" + client_id);
    client.run();
    return 0;
}
