#include <iostream>
#include <string>
#include <vector>
#include <unordered_set>
#include <chrono>
#include <ctime>
#include <thread>
#include <cstdlib>
#include <zmq.hpp>
#include "chat.pb.h"

class Client {
private:
    std::string user;
    std::string user_id;
    zmq::context_t context;
    zmq::socket_t req_socket;
    zmq::socket_t sub_socket;
    std::unordered_set<std::string> subscribed;

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
        req_socket.send(zmq::buffer(out), zmq::send_flags::none);
        std::cout << "[SEND] user=" << user << " id=" << req.id() << " ts=" << req.timestamp() << std::endl;

        zmq::message_t reply;
        req_socket.recv(reply, zmq::recv_flags::none);

        chat::Response res;
        res.ParseFromString(reply.to_string());
        std::cout << "[RECV] user=" << user << " id=" << res.id() << " ts=" << res.timestamp() << std::endl;
        return res;
    }

    void listen_sub() {
        while (true) {
            zmq::message_t topic_msg;
            zmq::message_t data_msg;
            if (!sub_socket.recv(topic_msg, zmq::recv_flags::none)) continue;
            if (!sub_socket.recv(data_msg, zmq::recv_flags::none)) continue;

            chat::Publication pub;
            pub.ParseFromString(data_msg.to_string());
            int64_t recv_ts = get_timestamp();

            std::cout << "[SUB " << user << "] canal=" << pub.channel()
                      << " msg=\"" << pub.content() << "\""
                      << " ts_envio=" << pub.timestamp()
                      << " ts_recebimento=" << recv_ts << std::endl;
        }
    }

public:
    Client(const std::string& broker_uri, const std::string& proxy_uri, const std::string& name)
        : user(name),
          user_id(gen_id()),
          context(1),
          req_socket(context, zmq::socket_type::req),
          sub_socket(context, zmq::socket_type::sub) {
        req_socket.connect(broker_uri);
        sub_socket.connect(proxy_uri);
    }

    void login() {
        for (int attempt = 1; attempt <= 5; ++attempt) {
            chat::Request req;
            auto* login = req.mutable_login();
            login->set_id(user_id);
            login->set_user(user);

            chat::Response res = send_req(req);
            if (res.login().status()) {
                std::cout << "[INFO] " << res.login().message() << std::endl;
                return;
            }
            std::cout << "[WARN] " << res.login().message() << " Retrying (" << attempt << "/5)..." << std::endl;
            user_id = gen_id();
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

    void subscribe_channel(const std::string& channel) {
        if (subscribed.insert(channel).second) {
            sub_socket.set(zmq::sockopt::subscribe, channel);
            std::cout << "[INFO] Subscribed to channel: " << channel << std::endl;
        }
    }

    void publish(const std::string& channel, const std::string& text) {
        chat::Request req;
        auto* pub = req.mutable_publish();
        pub->set_id(gen_id());
        pub->set_user(user);
        pub->set_channel(channel);
        pub->set_content(text);

        chat::Response res = send_req(req);
        std::cout << "[INFO] Publish to '" << channel << "': " << res.publish().message() << std::endl;
    }

    void run() {
        login();
        std::thread sub_thread(&Client::listen_sub, this);
        sub_thread.detach();

        int count = 1;
        while (true) {
            auto channels = list_channels();

            if (channels.size() < 5) {
                create_channel("canal_" + user + "_" + std::to_string(count++));
                channels = list_channels();
            }

            if (subscribed.size() < 3 && !channels.empty()) {
                for (const auto& ch : channels) {
                    if (subscribed.find(ch) == subscribed.end()) {
                        subscribe_channel(ch);
                        break;
                    }
                }
            }

            if (!channels.empty()) {
                std::string target = channels[std::rand() % channels.size()];
                for (int i = 1; i <= 10; ++i) {
                    publish(target, "Msg " + std::to_string(i) + " from " + user);
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            }
        }
    }
};

int main() {
    const char* host = std::getenv("HOSTNAME");
    std::string id = host ? host : "local";
    std::srand(std::time(nullptr));

    Client client("tcp://broker:5555", "tcp://proxy:5558", "bot_cpp_" + id);
    client.run();
    return 0;
}
