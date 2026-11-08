#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <zmq.hpp>
#include "chat.pb.h"

class Client {
private:
    std::string user;
    std::string user_id;
    zmq::context_t context;
    zmq::socket_t socket;

    int64_t get_timestamp() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }

    std::string gen_id() {
        return std::to_string(get_timestamp()) + "_" + std::to_string(std::rand() % 10000);
    }

    chat::Response send_req(chat::Request& req) {
        req.set_id(gen_id());
        req.set_timestamp(get_timestamp());

        std::string out;
        req.SerializeToString(&out);
        socket.send(zmq::buffer(out), zmq::send_flags::none);
        std::cout << "[SEND] user=" << user << " id=" << req.id() << " ts=" << req.timestamp() << std::endl;

        zmq::message_t reply;
        socket.recv(reply, zmq::recv_flags::none);

        chat::Response res;
        res.ParseFromString(reply.to_string());
        std::cout << "[RECV] user=" << user << " id=" << res.id() << " ts=" << res.timestamp() << std::endl;
        return res;
    }

public:
    Client(const std::string& uri, const std::string& name)
        : user(name), user_id(gen_id()), context(1), socket(context, zmq::socket_type::req) {
        socket.connect(uri);
    }

    void login() {
        while (true) {
            chat::Request req;
            auto* login = req.mutable_login();
            login->set_id(user_id);
            login->set_user(user);

            chat::Response res = send_req(req);
            if (res.login().status()) {
                std::cout << "[INFO] " << res.login().message() << std::endl;
                break;
            }
            std::cout << "[WARN] " << res.login().message() << " Retrying..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }

    std::vector<std::string> list_channels() {
        chat::Request req;
        req.mutable_list();
        chat::Response res = send_req(req);

        std::vector<std::string> channels;
        for (const auto& ch : res.list().channels()) {
            channels.push_back(ch);
        }
        return channels;
    }

    void create_channel(const std::string& name) {
        chat::Request req;
        auto* ch = req.mutable_channel();
        ch->set_id(gen_id());
        ch->set_name(name);

        chat::Response res = send_req(req);
        std::cout << "[INFO] Channel '" << name << "': " << res.channel().message() << std::endl;
    }

    void run() {
        login();

        int count = 1;
        while (true) {
            auto channels = list_channels();
            if (channels.size() < 5) {
                create_channel("canal_" + user + "_" + std::to_string(count++));
            }
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }
};

int main() {
    const char* host = std::getenv("HOSTNAME");
    std::string id = host ? host : "local";

    Client client("tcp://broker:5555", "bot_cpp_" + id);
    client.run();
    return 0;
}
