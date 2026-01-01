#pragma once

// Integration tests for sync - mirrors Swift's SyncTests.swift
// Uses Crow (server) + websocketpp (client) for real WebSocket communication

#include <LatticeCpp.hpp>
#include <iostream>
#include <thread>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <set>

// Asio and networking (define guard to avoid redefinition)
#ifndef ASIO_STANDALONE
#define ASIO_STANDALONE
#endif

#include <asio.hpp>
#include <crow_all.h>

#include <websocketpp/config/asio_no_tls_client.hpp>
#include <websocketpp/client.hpp>

// ============================================================================
// Test Model (defined outside namespace for LATTICE_SCHEMA macro compatibility)
// ============================================================================

struct SyncPerson {
    std::string name;
    int age;
};
LATTICE_SCHEMA(SyncPerson, name, age);

// Models for List<T> relationship testing
struct SyncChild {
    std::string name;
};
LATTICE_SCHEMA(SyncChild, name);

struct SyncParent {
    std::string name;
    std::vector<SyncChild*> children;  // List<SyncChild>
};
LATTICE_SCHEMA(SyncParent, name, children);

namespace lattice_integration_tests {

// ============================================================================
// Real WebSocket Client using websocketpp
// ============================================================================

class real_websocket_client : public lattice::websocket_client {
public:
    using client_t = websocketpp::client<websocketpp::config::asio_client>;
    using message_ptr = websocketpp::config::asio_client::message_type::ptr;

    real_websocket_client() {
        client_.clear_access_channels(websocketpp::log::alevel::all);
        client_.clear_error_channels(websocketpp::log::elevel::all);
        client_.init_asio();

        client_.set_open_handler([this](websocketpp::connection_hdl hdl) {
            hdl_ = hdl;
            state_ = lattice::websocket_state::open;
            if (on_open_) on_open_();
        });

        client_.set_message_handler([this](websocketpp::connection_hdl, message_ptr msg) {
            if (on_message_) {
                on_message_(lattice::websocket_message::from_string(msg->get_payload()));
            }
        });

        client_.set_fail_handler([this](websocketpp::connection_hdl) {
            state_ = lattice::websocket_state::closed;
            if (on_error_) on_error_("Connection failed");
        });

        client_.set_close_handler([this](websocketpp::connection_hdl) {
            state_ = lattice::websocket_state::closed;
            if (on_close_) on_close_(1000, "Connection closed");
        });
    }

    ~real_websocket_client() {
        disconnect();
        if (io_thread_.joinable()) {
            io_thread_.join();
        }
    }

    void connect(const std::string& url,
                const std::map<std::string, std::string>& headers = {}) override {
        websocketpp::lib::error_code ec;
        auto con = client_.get_connection(url, ec);
        if (ec) {
            if (on_error_) on_error_(ec.message());
            return;
        }

        // Add headers
        for (const auto& [key, value] : headers) {
            con->append_header(key, value);
        }

        client_.connect(con);

        // Run io_context in background thread
        io_thread_ = std::thread([this]() {
            client_.run();
        });
    }

    void disconnect() override {
        if (state_ == lattice::websocket_state::open) {
            websocketpp::lib::error_code ec;
            client_.close(hdl_, websocketpp::close::status::normal, "Client disconnect", ec);
        }
        client_.stop();
    }

    lattice::websocket_state state() const override { return state_; }

    void send(const lattice::websocket_message& message) override {
        if (state_ != lattice::websocket_state::open) return;

        websocketpp::lib::error_code ec;
        if (message.msg_type == lattice::websocket_message::type::binary) {
            client_.send(hdl_, message.data.data(), message.data.size(),
                        websocketpp::frame::opcode::binary, ec);
        } else {
            client_.send(hdl_, message.as_string(), websocketpp::frame::opcode::text, ec);
        }
    }

    void set_on_open(on_open_handler handler) override { on_open_ = std::move(handler); }
    void set_on_message(on_message_handler handler) override { on_message_ = std::move(handler); }
    void set_on_error(on_error_handler handler) override { on_error_ = std::move(handler); }
    void set_on_close(on_close_handler handler) override { on_close_ = std::move(handler); }

private:
    client_t client_;
    websocketpp::connection_hdl hdl_;
    std::thread io_thread_;
    std::atomic<lattice::websocket_state> state_{lattice::websocket_state::closed};

    on_open_handler on_open_;
    on_message_handler on_message_;
    on_error_handler on_error_;
    on_close_handler on_close_;
};

// Factory for real WebSocket clients
class real_network_factory : public lattice::network_factory {
public:
    std::unique_ptr<lattice::http_client> create_http_client() override {
        return nullptr;  // Not implemented yet
    }

    std::unique_ptr<lattice::websocket_client> create_websocket_client() override {
        return std::make_unique<real_websocket_client>();
    }
};

// ============================================================================
// Test Server using Crow (like LatticeServerKit in Swift)
// ============================================================================

class test_sync_server {
public:
    test_sync_server(int port = 18080) : port_(port) {
        // Server has its own database
        // Note: SyncPerson schema is already registered via LATTICE_SCHEMA macro
        // and ensure_tables() is called in lattice_db constructor
        server_db_ = std::make_unique<lattice::lattice_db>();
    }

    void start() {
        // Use CROW_WEBSOCKET_ROUTE macro
        CROW_WEBSOCKET_ROUTE(app_, "/sync")
            .onopen([this](crow::websocket::connection& conn) {
                std::lock_guard<std::mutex> lock(clients_mutex_);
                clients_.insert(&conn);
                std::cout << "    [Server] Client connected" << std::endl;

                // Send any existing events to the new client (catch-up)
                auto events = lattice::query_audit_log(server_db_->db(), false);
                if (!events.empty()) {
                    auto event = lattice::server_sent_event::make_audit_log(events);
                    conn.send_text(event.to_json());
                }
            })
            .onclose([this](crow::websocket::connection& conn, const std::string& reason) {
                std::lock_guard<std::mutex> lock(clients_mutex_);
                clients_.erase(&conn);
                std::cout << "    [Server] Client disconnected: " << reason << std::endl;
            })
            .onmessage([this](crow::websocket::connection& conn, const std::string& data, bool is_binary) {
                handle_message(conn, data);
            });

        server_thread_ = std::thread([this]() {
            app_.port(port_).run();
        });

        // Wait for server to start
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        running_ = true;
        std::cout << "    [Server] Started on port " << port_ << std::endl;
    }

    void stop() {
        if (running_) {
            app_.stop();
            if (server_thread_.joinable()) {
                server_thread_.join();
            }
            running_ = false;
            std::cout << "    [Server] Stopped" << std::endl;
        }
    }

    int port() const { return port_; }

    lattice::lattice_db& db() { return *server_db_; }

private:
    void handle_message(crow::websocket::connection& sender, const std::string& data) {
        auto event = lattice::server_sent_event::from_json(data);
        if (!event) {
            std::cout << "    [Server] Failed to parse message" << std::endl;
            return;
        }

        if (event->event_type == lattice::server_sent_event::type::audit_log) {
            std::cout << "    [Server] Received " << event->audit_logs.size() << " audit entries" << std::endl;
            for (const auto& e : event->audit_logs) {
                std::cout << "      - " << e.operation << " on " << e.table_name
                          << " (" << e.changed_fields.size() << " fields)" << std::endl;
            }

            // Apply changes to server database
            std::vector<std::string> ack_ids;
            for (const auto& entry : event->audit_logs) {
                // Check for duplicates
                auto existing = server_db_->db().query(
                    "SELECT id FROM AuditLog WHERE globalId = ?",
                    {entry.global_id}
                );
                if (!existing.empty()) continue;

                // If it's a link table (starts with _), ensure it exists
                if (!entry.table_name.empty() && entry.table_name[0] == '_') {
                    server_db_->ensure_link_table(entry.table_name);
                }

                // Apply the instruction
                auto [sql, params] = entry.generate_instruction();
                if (!sql.empty()) {
                    try {
                        server_db_->db().execute(sql, params);
                    } catch (const std::exception& e) {
                        std::cout << "    [Server] Error applying change: " << e.what() << std::endl;
                    }
                }

                // Store in AuditLog
                server_db_->db().execute(R"(
                    INSERT INTO AuditLog (globalId, tableName, operation, rowId, globalRowId,
                        changedFields, changedFieldsNames, timestamp, isFromRemote, isSynchronized)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
                )", {
                    entry.global_id,
                    entry.table_name,
                    entry.operation,
                    entry.row_id,
                    entry.global_row_id,
                    entry.changed_fields_to_json(),
                    entry.changed_fields_names_to_json(),
                    entry.timestamp
                });

                ack_ids.push_back(entry.global_id);
            }

            // Send ack back to sender
            if (!ack_ids.empty()) {
                auto ack = lattice::server_sent_event::make_ack(ack_ids);
                sender.send_text(ack.to_json());
            }

            // Broadcast to other clients
            {
                std::lock_guard<std::mutex> lock(clients_mutex_);
                for (auto* client : clients_) {
                    if (client != &sender) {
                        client->send_text(data);
                    }
                }
            }
        }
    }

    crow::SimpleApp app_;
    std::thread server_thread_;
    std::atomic<bool> running_{false};
    int port_;

    std::unique_ptr<lattice::lattice_db> server_db_;

    std::mutex clients_mutex_;
    std::set<crow::websocket::connection*> clients_;
};

// ============================================================================
// Integration Tests
// ============================================================================

inline void test_BasicSync() {
    std::cout << "Testing BasicSync integration..." << std::endl;

    // Start server
    test_sync_server server(18080);
    server.start();

    // Use real network factory
    auto factory = std::make_shared<real_network_factory>();
    lattice::set_network_factory(factory);

    // Client 1 - sync is integrated into configuration (like Swift's Lattice)
    lattice::configuration config1(
        ":memory:",                           // path
        "ws://localhost:18080/sync",          // websocket_url
        "client1"                             // authorization_token
    );
    lattice::lattice_db db1(config1);  // Sync auto-connects!

    // Client 2
    lattice::configuration config2(
        ":memory:",
        "ws://localhost:18080/sync",
        "client2"
    );
    lattice::lattice_db db2(config2);

    // Wait for connections (sync connects automatically in constructor)
    std::cout << "  Waiting for clients to connect..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    if (!db1.is_sync_connected() || !db2.is_sync_connected()) {
        std::cout << "  FAILED: Clients did not connect (db1="
                  << db1.is_sync_connected() << ", db2=" << db2.is_sync_connected() << ")" << std::endl;
        server.stop();
        return;
    }
    std::cout << "    Both clients connected" << std::endl;

    // -------------------------------------------------------------------------
    // Test: Create object on client 1, verify it syncs to client 2
    // -------------------------------------------------------------------------
    std::cout << "  Creating object on client 1..." << std::endl;

    auto person = db1.add(SyncPerson{"Alice", 30});

    std::string alice_global_id = person.global_id();
    std::cout << "    Created Alice with globalId: " << alice_global_id << std::endl;

    // Trigger sync (using db method instead of separate synchronizer)
    db1.sync_now();

    // Wait for sync to propagate
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Verify client 2 received the object
    auto persons_on_2 = db2.objects<SyncPerson>();
    std::cout << "    Client 2 has " << persons_on_2.size() << " persons" << std::endl;

    bool found_alice = false;
    for (const auto& p : persons_on_2) {
        // Note: implicit conversion works, no need for .detach()
        std::string name = p.name;
        int age = p.age;
        if (name == "Alice" && age == 30) {
            found_alice = true;
            break;
        }
    }

    if (found_alice) {
        std::cout << "    SUCCESS: Alice synced to client 2" << std::endl;
    } else {
        std::cout << "    FAILED: Alice not found on client 2" << std::endl;
    }

    // -------------------------------------------------------------------------
    // Test: Update object on client 1, verify it syncs to client 2
    // -------------------------------------------------------------------------
    std::cout << "  Updating Alice's age on client 1..." << std::endl;
    person.age = 31;

    db1.sync_now();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto alice_on_2 = db2.find_by_global_id<SyncPerson>(alice_global_id);
    if (alice_on_2 && alice_on_2->age == 31) {
        std::cout << "    SUCCESS: Update synced to client 2" << std::endl;
    } else {
        std::cout << "    FAILED: Update not synced" << std::endl;
    }

    // -------------------------------------------------------------------------
    // Test: Delete object on client 1, verify it syncs to client 2
    // -------------------------------------------------------------------------
    std::cout << "  Deleting Alice on client 1..." << std::endl;
    db1.remove(person);

    db1.sync_now();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto alice_after_delete = db2.find_by_global_id<SyncPerson>(alice_global_id);
    if (!alice_after_delete) {
        std::cout << "    SUCCESS: Delete synced to client 2" << std::endl;
    } else {
        std::cout << "    FAILED: Delete not synced" << std::endl;
    }

    // Cleanup - disconnect happens automatically in destructor
    db1.disconnect_sync();
    db2.disconnect_sync();
    server.stop();

    std::cout << "  BasicSync integration test completed!" << std::endl;
}

/// Test that List<T> relationships sync properly between clients.
/// This test verifies that when a parent object with children is created on one client,
/// the relationship (not just the objects) syncs to the other client.
inline void test_ListRelationshipSync() {
    std::cout << "Testing ListRelationshipSync integration..." << std::endl;

    // Start server
    test_sync_server server(18081);  // Different port to avoid conflicts
    server.start();

    // Use real network factory
    auto factory = std::make_shared<real_network_factory>();
    lattice::set_network_factory(factory);

    // Client 1 - using integrated sync configuration
    lattice::configuration config1(
        ":memory:",
        "ws://localhost:18081/sync",
        "client1"
    );
    lattice::lattice_db db1(config1);

    // Client 2
    lattice::configuration config2(
        ":memory:",
        "ws://localhost:18081/sync",
        "client2"
    );
    lattice::lattice_db db2(config2);

    // Wait for connections (sync connects automatically)
    std::cout << "  Waiting for clients to connect..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    if (!db1.is_sync_connected() || !db2.is_sync_connected()) {
        std::cout << "  FAILED: Clients did not connect (db1="
                  << db1.is_sync_connected() << ", db2=" << db2.is_sync_connected() << ")" << std::endl;
        server.stop();
        return;
    }
    std::cout << "    Both clients connected" << std::endl;

    // -------------------------------------------------------------------------
    // Test: Create parent with children on client 1, verify relationship syncs
    // -------------------------------------------------------------------------
    std::cout << "  Creating parent with children on client 1..." << std::endl;

    // Create parent
    auto parent = db1.add(SyncParent{"Parent", {}});
    std::string parent_global_id = parent.global_id();
    std::cout << "    Created Parent with globalId: " << parent_global_id << std::endl;

    // Create children and add to parent's list
    SyncChild child1_data{"Child1"};
    parent.children.push_back(child1_data);

    SyncChild child2_data{"Child2"};
    parent.children.push_back(child2_data);

    // Verify local state
    size_t local_children_count = parent.children.size();
    std::cout << "    Parent has " << local_children_count << " children locally" << std::endl;

    if (local_children_count != 2) {
        std::cout << "    FAILED: Expected 2 children, got " << local_children_count << std::endl;
        db1.disconnect_sync();
        db2.disconnect_sync();
        server.stop();
        return;
    }

    // Check local database state
    auto local_children = db1.objects<SyncChild>();
    std::cout << "    Client 1 has " << local_children.size() << " SyncChild objects in DB" << std::endl;

    // Check that AuditLog entries were created for the link table
    // This is key - link table changes should generate audit log entries
    auto audit_rows = db1.db().query("SELECT * FROM AuditLog");
    std::cout << "    Total audit logs on client 1: " << audit_rows.size() << std::endl;

    int link_table_logs = 0;
    for (const auto& row : audit_rows) {
        auto table_it = row.find("tableName");
        if (table_it != row.end() && std::holds_alternative<std::string>(table_it->second)) {
            std::string table_name = std::get<std::string>(table_it->second);
            auto op_it = row.find("operation");
            std::string op = op_it != row.end() && std::holds_alternative<std::string>(op_it->second)
                           ? std::get<std::string>(op_it->second) : "?";
            std::cout << "      - " << table_name << ": " << op << std::endl;

            if (table_name.find("_SyncParent_SyncChild") != std::string::npos) {
                link_table_logs++;
            }
        }
    }

    std::cout << "    Link table audit logs: " << link_table_logs << std::endl;

    if (link_table_logs < 2) {
        std::cout << "    WARNING: Expected at least 2 link table INSERT logs for sync to work. Found: " << link_table_logs << std::endl;
    }

    // Trigger sync
    std::cout << "  Triggering sync..." << std::endl;
    db1.sync_now();

    // Wait for sync to propagate
    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    // -------------------------------------------------------------------------
    // Verify objects synced to client 2
    // -------------------------------------------------------------------------
    auto parents_on_2 = db2.objects<SyncParent>();
    auto children_on_2 = db2.objects<SyncChild>();

    std::cout << "    Client 2 has " << parents_on_2.size() << " SyncParent objects" << std::endl;
    std::cout << "    Client 2 has " << children_on_2.size() << " SyncChild objects" << std::endl;

    if (parents_on_2.size() != 1) {
        std::cout << "    FAILED: Expected 1 parent on client 2, got " << parents_on_2.size() << std::endl;
        db1.disconnect_sync();
        db2.disconnect_sync();
        server.stop();
        return;
    }
    std::cout << "    SUCCESS: Parent synced to client 2" << std::endl;

    if (children_on_2.size() != 2) {
        std::cout << "    FAILED: Expected 2 children on client 2, got " << children_on_2.size() << std::endl;
        db1.disconnect_sync();
        db2.disconnect_sync();
        server.stop();
        return;
    }
    std::cout << "    SUCCESS: Children synced to client 2" << std::endl;

    // -------------------------------------------------------------------------
    // KEY TEST: Verify the relationship (link table) synced, not just objects
    // -------------------------------------------------------------------------
    auto synced_parent = db2.find_by_global_id<SyncParent>(parent_global_id);
    if (!synced_parent) {
        std::cout << "    FAILED: Could not find synced parent by globalId" << std::endl;
        db1.disconnect_sync();
        db2.disconnect_sync();
        server.stop();
        return;
    }

    size_t synced_children_count = synced_parent->children.size();
    std::cout << "    Synced parent has " << synced_children_count << " children via relationship" << std::endl;

    if (synced_children_count != 2) {
        std::cout << "    FAILED: Expected parent-child relationship to sync (2 children), got " << synced_children_count << std::endl;
        db1.disconnect_sync();
        db2.disconnect_sync();
        server.stop();
        return;
    }
    std::cout << "    SUCCESS: Parent-child relationship synced (List<T> links work!)" << std::endl;

    // Verify the correct children are linked
    bool found_child1 = false;
    bool found_child2 = false;
    for (auto& child : synced_parent->children) {
        std::string child_name = child.name.detach();
        if (child_name == "Child1") found_child1 = true;
        if (child_name == "Child2") found_child2 = true;
    }

    if (found_child1 && found_child2) {
        std::cout << "    SUCCESS: Both Child1 and Child2 are linked to parent" << std::endl;
    } else {
        std::cout << "    FAILED: Missing children - Child1=" << found_child1 << ", Child2=" << found_child2 << std::endl;
    }

    // -------------------------------------------------------------------------
    // Test: Remove a child from the relationship, verify sync
    // -------------------------------------------------------------------------
    std::cout << "  Removing Child1 from parent's children list on client 1..." << std::endl;

    // Find Child1 in parent's children and remove it
    for (auto& child : parent.children) {
        if (child.name.detach() == "Child1") {
            parent.children.erase(&child);
            break;
        }
    }

    // Verify local state
    size_t after_remove_count = parent.children.size();
    std::cout << "    Parent now has " << after_remove_count << " children locally" << std::endl;

    // Trigger sync
    db1.sync_now();
    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    // Verify on client 2
    auto synced_parent2 = db2.find_by_global_id<SyncParent>(parent_global_id);
    if (synced_parent2) {
        size_t remote_count = synced_parent2->children.size();
        if (remote_count == 1) {
            std::cout << "    SUCCESS: Relationship removal synced (1 child remaining)" << std::endl;
        } else {
            std::cout << "    FAILED: Expected 1 child after removal, got " << remote_count << std::endl;
        }
    }

    // Cleanup - disconnect happens automatically in destructor, but explicit is cleaner
    db1.disconnect_sync();
    db2.disconnect_sync();
    server.stop();

    std::cout << "  ListRelationshipSync integration test completed!" << std::endl;
}

} // namespace lattice_integration_tests
