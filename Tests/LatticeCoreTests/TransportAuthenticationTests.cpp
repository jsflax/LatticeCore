// Focused actual-Core transport admission. Every network transport is a local
// recording fixture; no external service or credential is used. TMPDIR owns files.
#ifndef SQLITE_CORE
#define SQLITE_CORE 1
#endif
#include <gtest/gtest.h>
#include <lattice.hpp>
#include <lattice/network.hpp>
#include <filesystem>
#include <chrono>
#include <atomic>

namespace {
using namespace lattice;
struct File {
    std::string path;
    File() {
        static std::atomic<unsigned> sequence{0};
        path = (std::filesystem::temp_directory_path() / ("transport-auth-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
            std::to_string(sequence++) + ".sqlite")).string();
    }
    ~File() { std::error_code ignored; for (auto suffix : {"", "-wal", "-shm", ".sync.lock"})
        std::filesystem::remove(path + suffix, ignored); }
};
struct Attempt { std::string url; HeadersMap headers; };
class RecordingTransport : public mock_sync_transport {
    std::shared_ptr<std::vector<Attempt>> attempts_;
public:
    explicit RecordingTransport(std::shared_ptr<std::vector<Attempt>> attempts) : attempts_(std::move(attempts)) {}
    void connect(const std::string& url, const HeadersMap& headers) override {
        attempts_->push_back({url, headers});
        // Keep CONNECTING: no upload/reconnect thread behavior is needed here.
    }
};
class RecordingFactory : public network_factory {
public:
    std::shared_ptr<std::vector<Attempt>> attempts = std::make_shared<std::vector<Attempt>>();
    std::unique_ptr<http_client> create_http_client() override { return std::make_unique<null_http_client>(); }
    std::unique_ptr<sync_transport> create_sync_transport() override { return std::make_unique<RecordingTransport>(attempts); }
};

struct NetworkScope {
    std::shared_ptr<network_factory> previous = get_network_factory();
    std::shared_ptr<RecordingFactory> recording = std::make_shared<RecordingFactory>();
    NetworkScope() { set_network_factory(recording); }
    ~NetworkScope() { set_network_factory(previous); }
};

TEST(TransportAuthentication, DefaultRemainsTokenRequiredAndModeIsCopiedIntoSwiftConfiguration) {
    configuration config;
    EXPECT_EQ(config.sync_authentication_mode, sync_authentication::bearer_token);
    config.websocket_url = "wss://fixture.invalid/browser/sync";
    EXPECT_FALSE(config.is_sync_enabled());
    config.authorization_token = "fixture-token";
    EXPECT_TRUE(config.is_sync_enabled());
    config.authorization_token.clear();
    config.sync_authentication_mode = sync_authentication::transport_managed;
    EXPECT_TRUE(config.is_sync_enabled());
    swift_configuration copied(config);
    EXPECT_EQ(copied.sync_authentication_mode, sync_authentication::transport_managed);
    EXPECT_TRUE(copied.is_sync_enabled());
    copied.websocket_url.clear();
    EXPECT_FALSE(copied.is_sync_enabled());
}

TEST(TransportAuthentication, CacheIdentitySeparatesAdmissionModeWithOtherwiseIdenticalConfiguration) {
    File file;
    swift_configuration bearer(file.path);
    bearer.audit_retention_seconds = 0;
    auto managed = bearer;
    managed.sync_authentication_mode = sync_authentication::transport_managed;
    EXPECT_NE(detail::connection_fingerprint(bearer), detail::connection_fingerprint(managed));
    auto& cache = detail::LatticeCache::instance();
    auto first = cache.get_or_create(bearer, SchemaVector{});
    auto second = cache.get_or_create(managed, SchemaVector{});
    EXPECT_NE(first.get(), second.get());
    EXPECT_EQ(second.get(), cache.get_or_create(managed, SchemaVector{}).get());
    second->close(); first->close();
}

TEST(TransportAuthentication, EmptyDefaultTokenNeverConstructsTransportButManagedModeConnectsWithoutAuthorization) {
    NetworkScope network;
    auto factory = network.recording;
    for (bool managed : {false, true}) {
        File file;
        configuration config(file.path);
        config.audit_retention_seconds = 0;
        config.websocket_url = "wss://fixture.invalid/browser/sync";
        if (managed) config.sync_authentication_mode = sync_authentication::transport_managed;
        lattice_db db(config);
        EXPECT_EQ(factory->attempts->size(), managed ? 1u : 0u);
        if (managed && !factory->attempts->empty()) {
            EXPECT_EQ(factory->attempts->front().url, config.websocket_url);
            EXPECT_TRUE(factory->attempts->front().headers.empty());
            db.connect_sync();
            EXPECT_EQ(factory->attempts->size(), 1u);
            db.reconnect_sync();
            EXPECT_EQ(factory->attempts->size(), 2u);
            EXPECT_TRUE(factory->attempts->back().headers.empty());
        }
        db.close();
    }
}

TEST(TransportAuthentication, LegacyBearerHeaderIsUnchanged) {
    NetworkScope network;
    auto factory = network.recording;
    File file;
    configuration config(file.path);
    config.audit_retention_seconds = 0;
    config.websocket_url = "wss://fixture.invalid/browser/sync";
    config.authorization_token = "synthetic-test-only";
    lattice_db db(config);
    ASSERT_EQ(factory->attempts->size(), 1u);
    EXPECT_EQ(factory->attempts->front().headers.at("Authorization"), "Bearer synthetic-test-only");
    db.close();
}
} // namespace
