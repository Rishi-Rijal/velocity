#include <asio.hpp>
#include <string>
#include <print>
#include <vector>
#include <iostream>
#include <string_view>
#include <thread>
#include <shared_mutex>
#include <functional> // for std::hash
#include "absl/container/flat_hash_map.h"

using asio::ip::tcp;
using common_token = asio::as_tuple_t<asio::use_awaitable_t<>>;
using tcp_acceptor = common_token::as_default_on_t<tcp::acceptor>;
using tcp_socket = common_token::as_default_on_t<tcp::socket>;

const size_t SHARD_COUNT = 64;

struct DB {
    struct alignas(64) Shard {
        absl::flat_hash_map<std::string, std::string> map;
        std::shared_mutex mutex;
    };

    std::vector<Shard> shards;

    DB() : shards(SHARD_COUNT) {
        for(auto& s : shards) s.map.reserve(4000);
    }

    //determine which shard owns the key
    size_t get_shard_index(std::string_view key) {
        return std::hash<std::string_view>{}(key) % SHARD_COUNT;
    }

    std::string get(std::string_view key) {
        size_t idx = get_shard_index(key);
        Shard& shard = shards[idx];

        std::shared_lock lock(shard.mutex);
        auto it = shard.map.find(key);
        if (it != shard.map.end()) return it->second;
        return "(empty)\n";
    }

    void set(std::string_view key, std::string_view value) {
        size_t idx = get_shard_index(key);
        Shard& shard = shards[idx];

        std::unique_lock lock(shard.mutex);
        shard.map.emplace(key, value);
    }
};

struct ParsedCommand {
    std::string_view cmd;
    std::string_view key;
    std::string_view value;
};

std::string_view trim_right(std::string_view s) {
    auto pos = s.find_last_not_of(" \n\r\t");
    if (pos == std::string_view::npos) return {};
    return s.substr(0, pos + 1);
}

ParsedCommand parse_line(std::string_view line) {
    line = trim_right(line);
    if (line.empty()) return {};

    ParsedCommand res;
    size_t first_space = line.find(' ');

    if (first_space == std::string_view::npos) {
        res.cmd = line;
        return res;
    }

    res.cmd = line.substr(0, first_space);
    size_t second_space = line.find(' ', first_space + 1);

    if (second_space == std::string_view::npos) {
        res.key = line.substr(first_space + 1);
    } else {
        res.key = line.substr(first_space + 1, second_space - first_space - 1);
        res.value = line.substr(second_space + 1);
    }
    return res;
}

asio::awaitable<void> handle_query(tcp_socket socket, DB& db) {
    try {
        socket.set_option(tcp::no_delay(true)); // Disable Nagle

        std::string buffer_storage;
        for (;;) {
            auto [read_err, n] = co_await asio::async_read_until(socket, asio::dynamic_buffer(buffer_storage, 1024), '\n');
            if (read_err) break;

            std::string_view line_view(buffer_storage.data(), n);
            auto parsed = parse_line(line_view);

            if (parsed.cmd == "set" && !parsed.key.empty()) {
                db.set(parsed.key, parsed.value);
            }
            else if (parsed.cmd == "get" && !parsed.key.empty()) {
                std::string res = db.get(parsed.key);
                co_await asio::async_write(socket, asio::buffer(res));
                co_await asio::async_write(socket, asio::buffer("\n"));
            }

            buffer_storage.erase(0, n);
        }
    } catch (...) {}
}

asio::awaitable<void> listener(DB& db) {
    auto executor = co_await asio::this_coro::executor;
    tcp_acceptor acceptor(executor, {tcp::v4(), 8999});
    acceptor.set_option(asio::socket_base::reuse_address(true));

    std::println("Server running on port 8999 (Sharded + Padded)");

    for (;;) {
        auto [err, socket] = co_await acceptor.async_accept(executor);
        if (!err) {
            asio::co_spawn(executor, handle_query(std::move(socket), db), asio::detached);
        }
    }
}

int main() {
    DB db;
    unsigned int thread_count = std::thread::hardware_concurrency();

    asio::io_context io_context(thread_count);
    asio::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto){ io_context.stop(); });

    asio::co_spawn(io_context, listener(db), asio::detached);

    std::vector<std::thread> threads;
    std::println("Starting {} worker threads...", thread_count);

    for (unsigned int i = 0; i < thread_count; ++i) {
        threads.emplace_back([&io_context] {
            io_context.run();
        });
    }

    io_context.run();

    for (auto& t : threads) t.join();
}