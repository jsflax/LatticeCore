#pragma once

#include <lattice/ipc.hpp>
#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <sys/socket.h>
#include <unistd.h>

namespace ipc_tests {

// ============================================================================
// test_resolve_ipc_socket_path — channel name resolves to correct path
// ============================================================================

void test_resolve_ipc_socket_path() {
    std::cout << "  test_resolve_ipc_socket_path..." << std::flush;

    auto path = lattice::resolve_ipc_socket_path("test_channel");

    // Should end with the channel name + .sock
    assert(path.find("test_channel.sock") != std::string::npos);

#ifdef __APPLE__
    // macOS: should contain Library/Caches/Lattice/ipc
    assert(path.find("Library/Caches/Lattice/ipc") != std::string::npos);
#else
    // Linux: should contain "lattice" somewhere in the path
    assert(path.find("lattice") != std::string::npos);
#endif

    // Directory should have been created
    auto dir = std::filesystem::path(path).parent_path();
    assert(std::filesystem::exists(dir));

    std::cout << " OK" << std::endl;
}

// ============================================================================
// test_length_prefix_framing — write + read round-trip
// ============================================================================

void test_length_prefix_framing() {
    std::cout << "  test_length_prefix_framing..." << std::flush;

    // Create a socketpair for testing
    int fds[2];
    int ret = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    assert(ret == 0);

    // Write a message
    std::string msg = "Hello, IPC!";
    bool ok = lattice::write_length_prefixed(fds[0], msg.data(), static_cast<uint32_t>(msg.size()));
    assert(ok);

    // Read it back
    auto payload = lattice::read_length_prefixed(fds[1]);
    assert(!payload.empty());
    std::string received(payload.begin(), payload.end());
    assert(received == msg);

    // Write a single byte message
    char one = 'Z';
    ok = lattice::write_length_prefixed(fds[0], &one, 1);
    assert(ok);
    payload = lattice::read_length_prefixed(fds[1]);
    assert(payload.size() == 1);
    assert(payload[0] == 'Z');

    // Large message (1MB) — needs concurrent read/write since socket
    // buffers are finite. This mirrors real IPC usage.
    std::string large(1'000'000, 'X');
    std::vector<uint8_t> large_result;
    std::thread reader([&]() {
        large_result = lattice::read_length_prefixed(fds[1]);
    });
    ok = lattice::write_length_prefixed(fds[0], large.data(), static_cast<uint32_t>(large.size()));
    assert(ok);
    reader.join();
    assert(large_result.size() == 1'000'000);
    assert(std::string(large_result.begin(), large_result.end()) == large);

    close(fds[0]);
    close(fds[1]);

    std::cout << " OK" << std::endl;
}

// ============================================================================
// test_ipc_server_client_connect — server accepts, client connects
// ============================================================================

void test_ipc_server_client_connect() {
    std::cout << "  test_ipc_server_client_connect..." << std::flush;

    auto socket_path = lattice::resolve_ipc_socket_path("test_connect");
    // Clean up from previous runs
    ::unlink(socket_path.c_str());

    std::atomic<bool> client_accepted{false};
    std::unique_ptr<lattice::ipc_socket_client> server_side_client;

    lattice::ipc_server server(socket_path);
    server.start([&](std::unique_ptr<lattice::ipc_socket_client> client) {
        server_side_client = std::move(client);
        client_accepted = true;
    });

    assert(server.is_listening());

    // Create a client and connect
    lattice::ipc_socket_client client(socket_path);

    std::atomic<bool> client_opened{false};
    client.set_on_open([&]() { client_opened = true; });
    client.set_on_message([](const lattice::transport_message&) {});
    client.set_on_error([](const std::string&) {});
    client.set_on_close([](int, const std::string&) {});

    client.connect("", {});

    // Wait for accept
    for (int i = 0; i < 100 && !client_accepted; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    assert(client_accepted);
    assert(client_opened);
    assert(client.state() == lattice::transport_state::open);
    assert(server_side_client != nullptr);

    // Clean up
    client.disconnect();
    server.stop();

    std::cout << " OK" << std::endl;
}

// ============================================================================
// test_ipc_message_exchange — bidirectional message passing
// ============================================================================

void test_ipc_message_exchange() {
    std::cout << "  test_ipc_message_exchange..." << std::flush;

    auto socket_path = lattice::resolve_ipc_socket_path("test_msg");
    ::unlink(socket_path.c_str());

    // Declare callbacks data FIRST so they outlive the transports
    std::mutex client_mu;
    std::vector<std::string> client_received;
    std::mutex server_mu;
    std::vector<std::string> server_received;

    std::unique_ptr<lattice::ipc_socket_client> server_transport;
    std::atomic<bool> accepted{false};

    lattice::ipc_server server(socket_path);
    server.start([&](std::unique_ptr<lattice::ipc_socket_client> client) {
        server_transport = std::move(client);
        accepted = true;
    });

    // Client connects
    lattice::ipc_socket_client client(socket_path);

    client.set_on_open([]() {});
    client.set_on_message([&](const lattice::transport_message& msg) {
        std::lock_guard<std::mutex> lock(client_mu);
        client_received.push_back(msg.as_string());
    });
    client.set_on_error([](const std::string&) {});
    client.set_on_close([](int, const std::string&) {});

    client.connect("", {});

    // Wait for accept
    for (int i = 0; i < 100 && !accepted; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    assert(accepted);

    // Set up server-side transport handlers and activate it
    server_transport->set_on_open([]() {});
    server_transport->set_on_message([&](const lattice::transport_message& msg) {
        std::lock_guard<std::mutex> lock(server_mu);
        server_received.push_back(msg.as_string());
    });
    server_transport->set_on_error([](const std::string&) {});
    server_transport->set_on_close([](int, const std::string&) {});

    // Start the server transport's read loop
    server_transport->connect("", {});

    // Client sends to server
    auto msg1 = lattice::transport_message::from_string("{\"kind\":\"auditLog\",\"data\":[]}");
    client.send(msg1);

    // Server sends to client
    auto msg2 = lattice::transport_message::from_string("{\"ack\":[\"id-1\",\"id-2\"]}");
    server_transport->send(msg2);

    // Wait for messages to arrive
    for (int i = 0; i < 100; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::lock_guard<std::mutex> lock1(server_mu);
        std::lock_guard<std::mutex> lock2(client_mu);
        if (!server_received.empty() && !client_received.empty()) break;
    }

    {
        std::lock_guard<std::mutex> lock(server_mu);
        assert(server_received.size() == 1);
        assert(server_received[0] == "{\"kind\":\"auditLog\",\"data\":[]}");
    }

    {
        std::lock_guard<std::mutex> lock(client_mu);
        assert(client_received.size() == 1);
        assert(client_received[0] == "{\"ack\":[\"id-1\",\"id-2\"]}");
    }

    // Clean up: disconnect transports before their callback data is destroyed
    client.disconnect();
    server_transport->disconnect();
    server.stop();

    std::cout << " OK" << std::endl;
}

// ============================================================================
// test_ipc_client_transport_interface — triggers on_open, on_message, on_close
// ============================================================================

void test_ipc_client_transport_interface() {
    std::cout << "  test_ipc_client_transport_interface..." << std::flush;

    auto socket_path = lattice::resolve_ipc_socket_path("test_interface");
    ::unlink(socket_path.c_str());

    std::unique_ptr<lattice::ipc_socket_client> server_transport;
    std::atomic<bool> accepted{false};

    lattice::ipc_server server(socket_path);
    server.start([&](std::unique_ptr<lattice::ipc_socket_client> client) {
        server_transport = std::move(client);
        accepted = true;
    });

    // Track all lifecycle events
    std::atomic<bool> got_open{false};
    std::atomic<bool> got_close{false};
    std::atomic<int> message_count{0};

    lattice::ipc_socket_client client(socket_path);
    client.set_on_open([&]() { got_open = true; });
    client.set_on_message([&](const lattice::transport_message&) { message_count++; });
    client.set_on_error([](const std::string&) {});
    client.set_on_close([&](int code, const std::string&) {
        got_close = true;
        assert(code == 1000);  // Normal closure
    });

    // Initially closed
    assert(client.state() == lattice::transport_state::closed);

    // Connect
    client.connect("", {});
    assert(got_open);
    assert(client.state() == lattice::transport_state::open);

    // Wait for server accept and set up
    for (int i = 0; i < 100 && !accepted; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    server_transport->set_on_open([]() {});
    server_transport->set_on_message([](const lattice::transport_message&) {});
    server_transport->set_on_error([](const std::string&) {});
    server_transport->set_on_close([](int, const std::string&) {});
    server_transport->connect("", {});

    // Send a couple of messages from server
    server_transport->send(lattice::transport_message::from_string("msg1"));
    server_transport->send(lattice::transport_message::from_string("msg2"));

    for (int i = 0; i < 100 && message_count < 2; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    assert(message_count == 2);

    // Disconnect triggers on_close
    client.disconnect();
    assert(got_close);
    assert(client.state() == lattice::transport_state::closed);

    server_transport->disconnect();
    server.stop();

    std::cout << " OK" << std::endl;
}

// ============================================================================
// test_ipc_endpoint_auto_negotiate — first process binds, second connects
// ============================================================================

void test_ipc_endpoint_auto_negotiate() {
    std::cout << "  test_ipc_endpoint_auto_negotiate..." << std::flush;

    // Clean up any stale socket
    auto socket_path = lattice::resolve_ipc_socket_path("test_negotiate");
    ::unlink(socket_path.c_str());

    // First endpoint should become the server
    lattice::ipc_endpoint ep1("test_negotiate");

    std::unique_ptr<lattice::ipc_socket_client> server_transport;
    std::atomic<bool> server_got_transport{false};

    ep1.start([&](std::unique_ptr<lattice::ipc_socket_client> t) {
        server_transport = std::move(t);
        server_got_transport = true;
    });

    assert(ep1.is_server());

    // Second endpoint should become the client
    lattice::ipc_endpoint ep2("test_negotiate");

    std::unique_ptr<lattice::ipc_socket_client> client_transport;
    std::atomic<bool> client_got_transport{false};

    ep2.start([&](std::unique_ptr<lattice::ipc_socket_client> t) {
        client_transport = std::move(t);
        client_got_transport = true;
    });

    assert(!ep2.is_server());
    assert(client_got_transport);  // Client transport returned immediately

    // Client transport should be connectable
    std::atomic<bool> client_open{false};
    client_transport->set_on_open([&]() { client_open = true; });
    client_transport->set_on_message([](const lattice::transport_message&) {});
    client_transport->set_on_error([](const std::string&) {});
    client_transport->set_on_close([](int, const std::string&) {});
    client_transport->connect("", {});

    // Wait for server to accept
    for (int i = 0; i < 100 && !server_got_transport; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    assert(server_got_transport);
    assert(client_open);
    assert(client_transport->state() == lattice::transport_state::open);

    // Set up server transport and verify bidirectional messaging
    std::atomic<int> server_msgs{0};
    std::atomic<int> client_msgs{0};

    server_transport->set_on_open([]() {});
    server_transport->set_on_message([&](const lattice::transport_message&) { server_msgs++; });
    server_transport->set_on_error([](const std::string&) {});
    server_transport->set_on_close([](int, const std::string&) {});
    server_transport->connect("", {});

    client_transport->set_on_message([&](const lattice::transport_message&) { client_msgs++; });

    // Bidirectional send
    client_transport->send(lattice::transport_message::from_string("from_client"));
    server_transport->send(lattice::transport_message::from_string("from_server"));

    for (int i = 0; i < 100 && (server_msgs < 1 || client_msgs < 1); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    assert(server_msgs >= 1);
    assert(client_msgs >= 1);

    // Clean up
    client_transport->disconnect();
    server_transport->disconnect();
    ep1.stop();
    ep2.stop();

    std::cout << " OK" << std::endl;
}

// ============================================================================
// test_ipc_connection_lost — read loop detects disconnect
// ============================================================================

void test_ipc_connection_lost() {
    std::cout << "  test_ipc_connection_lost..." << std::flush;

    auto socket_path = lattice::resolve_ipc_socket_path("test_lost");
    ::unlink(socket_path.c_str());

    std::unique_ptr<lattice::ipc_socket_client> server_transport;
    std::atomic<bool> accepted{false};

    lattice::ipc_server server(socket_path);
    server.start([&](std::unique_ptr<lattice::ipc_socket_client> client) {
        server_transport = std::move(client);
        accepted = true;
    });

    lattice::ipc_socket_client client(socket_path);
    std::atomic<bool> client_close_called{false};
    int close_code = 0;

    client.set_on_open([]() {});
    client.set_on_message([](const lattice::transport_message&) {});
    client.set_on_error([](const std::string&) {});
    client.set_on_close([&](int code, const std::string&) {
        close_code = code;
        client_close_called = true;
    });

    client.connect("", {});

    for (int i = 0; i < 100 && !accepted; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Abruptly disconnect the server side by closing its fd
    server_transport.reset();  // Destroys without calling disconnect — simulates crash
    server.stop();

    // Client should detect the lost connection
    for (int i = 0; i < 100 && !client_close_called; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    assert(client_close_called);
    assert(close_code == 1006);  // Connection lost

    client.disconnect();

    std::cout << " OK" << std::endl;
}

// ============================================================================
// End-to-end IPC sync tests (require lattice_db + model definitions)
// These use Person struct defined in LatticeCoreTests.cpp
// ============================================================================

template<typename PersonT>
void test_ipc_source_to_target() {
    std::cout << "  test_ipc_source_to_target..." << std::flush;

    std::string channel = "test_e2e_s2t";
    auto socket_path = lattice::resolve_ipc_socket_path(channel);
    ::unlink(socket_path.c_str());

    auto source_path = std::filesystem::temp_directory_path() / "ipc_source.sqlite";
    auto target_path = std::filesystem::temp_directory_path() / "ipc_target.sqlite";
    std::filesystem::remove(source_path);
    std::filesystem::remove(target_path);

    // Register mock network factory (required for lattice_db even if we don't use WSS)
    lattice::set_network_factory(std::make_shared<lattice::mock_network_factory>());

    // Source: opens first, becomes server
    lattice::configuration source_cfg(source_path.string());
    source_cfg.ipc_targets.push_back({channel, std::nullopt});
    lattice::lattice_db source(source_cfg);
    source.objects<PersonT>();

    // Target: opens second, becomes client
    lattice::configuration target_cfg(target_path.string());
    target_cfg.ipc_targets.push_back({channel, std::nullopt});
    lattice::lattice_db target(target_cfg);
    target.objects<PersonT>();

    // Wait for IPC connection to establish
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Insert on source
    source.add(PersonT{"Alice", 30, std::nullopt});

    // Wait for sync to propagate
    for (int i = 0; i < 100; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (target.objects<PersonT>().size() > 0) break;
    }

    auto results = target.objects<PersonT>();
    assert(results.size() == 1);
    assert(results[0].name.detach() == "Alice");
    assert(results[0].age.detach() == 30);

    source.close();
    target.close();
    std::filesystem::remove(source_path);
    std::filesystem::remove(target_path);

    std::cout << " OK" << std::endl;
}

template<typename PersonT>
void test_ipc_bidirectional() {
    std::cout << "  test_ipc_bidirectional..." << std::flush;

    std::string channel = "test_e2e_bidi";
    auto socket_path = lattice::resolve_ipc_socket_path(channel);
    ::unlink(socket_path.c_str());

    auto path_a = std::filesystem::temp_directory_path() / "ipc_bidi_a.sqlite";
    auto path_b = std::filesystem::temp_directory_path() / "ipc_bidi_b.sqlite";
    std::filesystem::remove(path_a);
    std::filesystem::remove(path_b);

    lattice::set_network_factory(std::make_shared<lattice::mock_network_factory>());

    lattice::configuration cfg_a(path_a.string());
    cfg_a.ipc_targets.push_back({channel, std::nullopt});
    lattice::lattice_db db_a(cfg_a);
    db_a.objects<PersonT>();

    lattice::configuration cfg_b(path_b.string());
    cfg_b.ipc_targets.push_back({channel, std::nullopt});
    lattice::lattice_db db_b(cfg_b);
    db_b.objects<PersonT>();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Insert on A, should appear on B
    db_a.add(PersonT{"FromA", 25, std::nullopt});

    for (int i = 0; i < 100; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (db_b.objects<PersonT>().size() > 0) break;
    }

    auto b_results = db_b.objects<PersonT>();
    assert(b_results.size() == 1);
    assert(b_results[0].name.detach() == "FromA");

    // Insert on B, should appear on A
    db_b.add(PersonT{"FromB", 35, std::nullopt});

    for (int i = 0; i < 100; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (db_a.objects<PersonT>().size() >= 2) break;
    }

    auto a_results = db_a.objects<PersonT>();
    assert(a_results.size() == 2);

    db_a.close();
    db_b.close();
    std::filesystem::remove(path_a);
    std::filesystem::remove(path_b);

    std::cout << " OK" << std::endl;
}

// ============================================================================
// Run all IPC tests
// ============================================================================

void run_all() {
    std::cout << std::endl;
    std::cout << "--- IPC Transport Tests ---" << std::endl;

    test_resolve_ipc_socket_path();
    test_length_prefix_framing();
    test_ipc_server_client_connect();
    test_ipc_message_exchange();
    test_ipc_client_transport_interface();
    test_ipc_endpoint_auto_negotiate();
    test_ipc_connection_lost();

    std::cout << "--- IPC Transport Tests: All passed ---" << std::endl;
}

} // namespace ipc_tests
