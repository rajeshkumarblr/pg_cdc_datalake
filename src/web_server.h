#pragma once

#include "metrics.h"
#include <atomic>
#include <string>

class WebServer {
public:
    WebServer(const std::string& config_path, Metrics& metrics, std::atomic<bool>& shutdown_flag);
    ~WebServer();

    void run(int port = 8080);
    void stop();

private:
    std::string config_path_;
    Metrics& metrics_;
    std::atomic<bool>& shutdown_flag_;
    
    // Pimpl idiom to hide httplib headers from the rest of the project compilation
    struct Impl;
    Impl* impl_;
};
