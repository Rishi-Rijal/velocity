#include <asio.hpp>
#include <string>
#include <print>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <iostream>

using asio::ip::tcp;
using std::println;
using common_token = asio::as_tuple_t<asio::use_awaitable_t<>>;
using tcp_acceptor = common_token::as_default_on_t<tcp::acceptor>;
using tcp_socket = common_token::as_default_on_t<tcp::socket>;
using Database = std::unordered_map<std::string, std::string>;


std::unordered_set<std::string> allowed_queries = {"set", "get"};

std::vector<std::string> parse_query(const std::string& message_buffer) {
    std::string clean_msg = message_buffer;
    if (!clean_msg.empty() && clean_msg.back() == '\r') clean_msg.pop_back();

    size_t firstSpace = clean_msg.find(' ');

    if (firstSpace == std::string::npos) return {};

    std::string cmd = clean_msg.substr(0, firstSpace);

    if (cmd == "set") {
        size_t secondSpace = clean_msg.find(' ', firstSpace + 1);

        if (secondSpace != std::string::npos) {
            std::string key = clean_msg.substr(firstSpace + 1, secondSpace - firstSpace - 1);
            std::string val = clean_msg.substr(secondSpace + 1);
            return {cmd, key, val};
        }
    }
    else if (cmd == "get") {
        std::string key = clean_msg.substr(firstSpace + 1);
        return {cmd, key};
    }

    return {};
}

void set_data(std::vector<std::string>& parsed_query, Database& db) {
    std::string key = parsed_query[1];

    db[key] = std::move(parsed_query[2]);

}

std::string get_data(const std::string& raw_key, Database& db) {
    std::string key = raw_key;

    key.erase(key.find_last_not_of(" \n\r\t") + 1);

    auto it = db.find(key);

    if (it != db.end()) {
        return it->second;
    }

    return "(empty)\n";
}

asio::awaitable<void> handle_query(tcp_socket socket, Database& db) {
    try {
        std::string data;
        std::string dir = "velocity#~ ";
        for (;;) {
            co_await asio::async_write(socket, asio::buffer(dir));
            auto [read_err, n] = co_await asio::async_read_until(socket, asio::dynamic_buffer(data, 1024), '\n');

            if (read_err) {
                if (read_err != asio::error::eof)
                    std::println("[ERROR] Read error: {}", read_err.message());
                break;
            }

            std::string line = data.substr(0, n);
            data.erase(0, n);

            std::vector<std::string> parsed_query = parse_query(line);
            if (parsed_query.empty()) {
                std::string msg = "invalid Query\n";
                co_await asio::async_write(socket, asio::buffer(msg));
                continue;
            }

            std::string user_request = parsed_query[0];
            if (user_request == "set") {
                if (parsed_query.size() < 3) {
                    std::string msg = "invalid Query\n";
                    co_await asio::async_write(socket, asio::buffer(msg));
                    continue;
                }
                set_data(parsed_query, db);
            }else if (user_request == "get") {
                if (parsed_query.size() < 2) {
                    std::string msg = "invalid Query\n";
                    co_await asio::async_write(socket, asio::buffer(msg));
                    continue;
                }
                std::string data = get_data(parsed_query[1], db);
                co_await asio::async_write(socket, asio::buffer(data));
                continue;

            }else {
                std::string msg = "invalid Query\n";
                co_await asio::async_write(socket, asio::buffer(msg));
                continue;
            }
        }
    } catch (std::exception& e) {
        std::println("Exception in handler: {}", e.what());
    }
}

asio::awaitable<void> listener(Database& db) {
    auto executor = co_await asio::this_coro::executor;
    tcp_acceptor acceptor(executor, {tcp::v4(), 8999});
    for (;;) {
       auto[accept_err, socket] = co_await acceptor.async_accept(executor);
        if (accept_err) {
            println("[ERROR] {}", accept_err.message());
        }
        asio::co_spawn(executor, handle_query(std::move(socket), db), asio::detached);
    }
}


int main(int argc, const char* argv[]) {
    Database db;

    asio::io_context io_context(1);
    asio::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto){ io_context.stop(); });

    co_spawn(io_context, listener(db), asio::detached);

    io_context.run();
}