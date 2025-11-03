#include <iostream>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 8080
#define BUFFER_SIZE 1024

bool running = true;
int g_client_fd;

void receive_messages() {
    char buffer[BUFFER_SIZE];
    
    while (running) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes_received = read(g_client_fd, buffer, BUFFER_SIZE - 1);
        
        if (bytes_received <= 0) {
            running = false;
            break;
        }
        
        std::cout << buffer;
    }
}

int main() {
    g_client_fd = socket(AF_INET, SOCK_STREAM, 0);
    
    sockaddr_in server_address = {};
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr);
    
    if (connect(g_client_fd, (sockaddr*)&server_address, sizeof(server_address)) == 0) {
        std::cout << "Connected to the server at 127.0.0.1:" << PORT << "\n";
    } else {
        std::cerr << "Failed to connect to the server.\n";
        return 1;
    }
    
    std::thread receiver_thread(receive_messages);
    
    std::string message;
    while (running) {
        std::getline(std::cin, message);
        
        if (message == "exit") {
            running = false;
            break;
        }
        
        send(g_client_fd, message.c_str(), message.size(), 0);
    }
    
    if (receiver_thread.joinable()) {
        receiver_thread.join();
    }
    close(g_client_fd);
    std::cout << "Client shut down.\n";
    
    return 0;
}