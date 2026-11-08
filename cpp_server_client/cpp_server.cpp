#include <iostream>
#include <string>
#include <chrono>
#include <fstream>
#include <unordered_set>
#include <zmq.hpp>
#include "chat.pb.h"

class Server {
private:
	zmq::context_t context;
	zmq::socket_t socket;
	std::unordered_set<std::string> users_active;
	std::unordered_set<std::string> channels_active;

	int64_t get_timestamp(){
		return std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::system_clock::now().time_since_epoch()
		).count();
	}

	void get_login(const chat::LoginRequest& req, chat::Response& res){
		std::string user = req.user();
		chat::Loginresponse* login_res = res.mutable_login();

		if(users_active.insert(user).second){
			login_res->set_status(true);
			login_res->set_message("Login Sucessfully.");
			std::cout << "[INFO] New session: " << user << std::endl;
		} else {
			login_res->set_status(false);
			login_res->set_message("Login Error.");
			std::cout << "[WARN] User already exists: " << USUARIO << std::endl;
		}
	}
}
