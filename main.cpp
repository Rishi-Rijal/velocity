#include <asio.hpp>
#include <string>
#include <print>

using asio::ip::tcp;
using std::println;
using common_token = asio::as_tuple_t<asio::use_awaitable_t<>>;
using tcp_acceptor = common_token::as_default_on_t<tcp::acceptor>;
using tcp_socket = common_token::as_default_on_t<tcp::socket>;


asio::awaitable<void> handle_query(tcp_socket socket) {
    char* message_buffer[256];
    std::string msg = "good\n";
    for (;;) {
        auto[read_err, read_msg_len] = co_await socket.async_read_some(asio::buffer(message_buffer));
        if (read_err) {
            println("[ERROR] {}", read_err.message());
            msg = read_err.message();
        }
        auto[write_err, write_msg_len] = co_await asio::async_write(socket, asio::buffer(msg));
        if (write_err) {
            println("[ERROR] {}", write_err.message());
        }
    }
}

asio::awaitable<void> listener() {
    auto executor = co_await asio::this_coro::executor;
    tcp_acceptor acceptor(executor, {tcp::v4(), 8999});
    for (;;) {
       auto[accept_err, socket] = co_await acceptor.async_accept(executor);
        if (accept_err) {
            println("[ERROR] {}", accept_err.message());
        }
        asio::co_spawn(executor, handle_query(std::move(socket)), asio::detached);
    }
}


int main(int argc, const char* argv[]) {
    std::unordered_map<std::string, std::string> bucket_map;

    asio::io_context io_context(1);
    asio::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto){ io_context.stop(); });

    co_spawn(io_context, listener(), asio::detached);

    io_context.run();
}