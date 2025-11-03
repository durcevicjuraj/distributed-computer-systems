#include <iostream>
#include <cstring>
#include <thread>
#include <vector>
#include <map>
#include <mutex>
#include <sstream>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define PORT 8080
#define BUFFER_SIZE 1024

std::map<std::string, std::vector<int> > topic_subscribers;
std::mutex topic_mutex;

void handle_client(int client_fd) {
    char buffer[BUFFER_SIZE];
    
    while (true) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes_received = read(client_fd, buffer, BUFFER_SIZE - 1);
        
        if (bytes_received <= 0) {
            std::cout << "Client disconnected.\n";
            break;
        }
        
        std::string command(buffer);
        std::istringstream iss(command);
        std::string cmd, topic, message;
        
        iss >> cmd;
        
        if (cmd == "SUBSCRIBE") {
            iss >> topic;
            std::lock_guard<std::mutex> lock(topic_mutex);
            topic_subscribers[topic].push_back(client_fd);
            std::cout << "Client subscribed to topic: " << topic << "\n";
            
        } else if (cmd == "UNSUBSCRIBE") {
            iss >> topic;
            std::lock_guard<std::mutex> lock(topic_mutex);
            
            if (topic_subscribers.find(topic) != topic_subscribers.end()) {
                std::vector<int>& subscribers = topic_subscribers[topic];
                for (size_t i = 0; i < subscribers.size(); i++) {
                    if (subscribers[i] == client_fd) {
                        subscribers.erase(subscribers.begin() + i);
                        std::cout << "Client unsubscribed from topic: " << topic << "\n";
                        std::string response = "Unsubscribed from topic: " + topic + "\n";
                        send(client_fd, response.c_str(), response.length(), 0);
                        break;
                    }
                }
            }
            
        } else if (cmd == "LIST") {
            std::string sub_cmd;
            iss >> sub_cmd;
            
            if (sub_cmd == "TOPICS") {
                std::lock_guard<std::mutex> lock(topic_mutex);
                std::string response = "Active topics:\n";
                
                if (topic_subscribers.empty()) {
                    response += "  (no topics yet)\n";
                } else {
                    std::map<std::string, std::vector<int> >::iterator it;
                    for (it = topic_subscribers.begin(); it != topic_subscribers.end(); ++it) {
                        response += "  - " + it->first + " (" + std::to_string(it->second.size()) + " subscribers)\n";
                    }
                }
                send(client_fd, response.c_str(), response.length(), 0);
                std::cout << "Client requested topic list\n";
            }
            
        } else if (cmd == "PUBLISH") {
            iss >> topic;
            std::getline(iss, message);
            if (!message.empty() && message[0] == ' ') {
                message = message.substr(1);
            }
            
            std::cout << "Publishing to topic '" << topic << "': " << message << "\n";
            
            std::lock_guard<std::mutex> lock(topic_mutex);
            if (topic_subscribers.find(topic) != topic_subscribers.end()) {
                std::string msg = "[" + topic + "] " + message + "\n";
                for (size_t i = 0; i < topic_subscribers[topic].size(); i++) {
                    int subscriber_fd = topic_subscribers[topic][i];
                    send(subscriber_fd, msg.c_str(), msg.length(), 0);
                }
                std::cout << "Message sent to " << topic_subscribers[topic].size() << " subscribers\n";
            } else {
                std::cout << "No subscribers for topic: " << topic << "\n";
            }
        }
    }
    
    {
        std::lock_guard<std::mutex> lock(topic_mutex);
        std::map<std::string, std::vector<int> >::iterator it;
        for (it = topic_subscribers.begin(); it != topic_subscribers.end(); ++it) {
            std::vector<int>& subscribers = it->second;
            for (size_t i = 0; i < subscribers.size(); i++) {
                if (subscribers[i] == client_fd) {
                    subscribers.erase(subscribers.begin() + i);
                    break;
                }
            }
        }
    }
    
    close(client_fd);
}

class ClientHandler {
    int fd;
public:
    ClientHandler(int client_fd) : fd(client_fd) {}
    void operator()() {
        handle_client(fd);
    }
};

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    
    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_port = htons(PORT);
    address.sin_addr.s_addr = INADDR_ANY;
    
    int option_value = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &option_value, sizeof(int));
    bind(server_fd, (sockaddr*)&address, sizeof(address));
    listen(server_fd, 3);
    
    std::cout << "Server is running on port " << PORT << " and waiting for connections...\n";
    
    while (true) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd >= 0) {
            std::cout << "Client connected.\n";
            ClientHandler handler(client_fd);
            std::thread t(handler);
            t.detach();
        }
    }
    
    close(server_fd);
    return 0;
}