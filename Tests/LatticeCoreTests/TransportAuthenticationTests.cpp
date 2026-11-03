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

namespace {
struct PinAdmission {
    bool supported = false;
    std::vector<std::string> pins;
    std::vector<Attempt> attempts;
};
class PinTransport : public mock_sync_transport {
    std::shared_ptr<PinAdmission> admission_;
public:
    explicit PinTransport(std::shared_ptr<PinAdmission> admission) : admission_(std::move(admission)) {}
    bool configure_certificate_pin(const std::string& pin) override {
        admission_->pins.push_back(pin);
        return pin.empty() || admission_->supported;
    }
    void connect(const std::string& url, const HeadersMap& headers) override {
        admission_->attempts.push_back({url, headers});
    }
};
class PinFactory : public network_factory {
public:
    std::shared_ptr<PinAdmission> admission = std::make_shared<PinAdmission>();
    std::unique_ptr<http_client> create_http_client() override { return std::make_unique<null_http_client>(); }
    std::unique_ptr<sync_transport> create_sync_transport() override { return std::make_unique<PinTransport>(admission); }
};
struct PinNetworkScope {
    std::shared_ptr<network_factory> previous = get_network_factory();
    std::shared_ptr<PinFactory> recording = std::make_shared<PinFactory>();
    PinNetworkScope() { set_network_factory(recording); }
    ~PinNetworkScope() { set_network_factory(previous); }
};

TEST(TransportAuthentication, PinUnsupportedDefaultsAndGenericCallbackFailClosed) {
    const std::string pin(64, 'a');
    mock_sync_transport unsupported;
    EXPECT_TRUE(unsupported.configure_certificate_pin(""));
    EXPECT_FALSE(unsupported.configure_certificate_pin(pin));
    PinAdmission admission;
    generic_sync_transport generic(&admission, nullptr, nullptr, nullptr, nullptr);
    EXPECT_TRUE(generic.configure_certificate_pin(""));
    EXPECT_FALSE(generic.configure_certificate_pin(pin));
    generic.set_certificate_pin_callback([](void* context, const void* value) -> bool {
        auto& seen = *static_cast<PinAdmission*>(context);
        seen.pins.push_back(*static_cast<const std::string*>(value));
        return seen.supported;
    });
    EXPECT_FALSE(generic.configure_certificate_pin(pin));
    admission.supported = true;
    EXPECT_TRUE(generic.configure_certificate_pin(pin));
    ASSERT_EQ(admission.pins.size(), 2u);
    EXPECT_EQ(admission.pins[0], pin);
    EXPECT_EQ(admission.pins[1], pin);
}

TEST(TransportAuthentication, PinIsForwardedBeforeConnectAndUnsupportedTransportNeverDials) {
    for (bool supported : {false, true}) {
        PinNetworkScope network;
        auto observed = network.recording->admission;
        observed->supported = supported;
        File file;
        configuration config(file.path);
        config.audit_retention_seconds = 0;
        config.websocket_url = "wss://fixture.invalid/sync";
        config.authorization_token = "fixture-token";
        config.pinned_server_certificate_sha256 = std::string(64, 'a');
        lattice_db db(config);
        ASSERT_EQ(observed->pins.size(), 1u);
        EXPECT_EQ(observed->pins.front(), config.pinned_server_certificate_sha256);
        EXPECT_EQ(observed->attempts.size(), supported ? 1u : 0u);
        if (supported) {
            EXPECT_EQ(observed->attempts.front().headers.at("Authorization"), "Bearer fixture-token");
            db.reconnect_sync();
            ASSERT_EQ(observed->pins.size(), 2u);
            EXPECT_EQ(observed->pins.back(), config.pinned_server_certificate_sha256);
            EXPECT_EQ(observed->attempts.size(), 2u);
        }
        db.close();
    }
}

TEST(TransportAuthentication, PinCannotAdmitAnInsecureWebSocketEvenIfTransportClaimsSupport) {
    PinNetworkScope network;
    network.recording->admission->supported = true;
    File file;
    configuration config(file.path);
    config.audit_retention_seconds = 0;
    config.websocket_url = "ws://fixture.invalid/sync";
    config.authorization_token = "fixture-token";
    config.pinned_server_certificate_sha256 = std::string(64, 'a');
    lattice_db db(config);
    EXPECT_TRUE(network.recording->admission->pins.empty());
    EXPECT_TRUE(network.recording->admission->attempts.empty());
    db.close();
}

TEST(TransportAuthentication, DifferentPinsNeverShareTheNativeCachedConnection) {
    File file;
    swift_configuration unpinned(file.path);
    unpinned.audit_retention_seconds = 0;
    unpinned.sync_autostart = false;
    unpinned.websocket_url = "wss://fixture.invalid/sync";
    unpinned.authorization_token = "fixture-token";
    auto pin_a = unpinned; pin_a.set_certificate_pin(std::string(64, 'a'));
    auto pin_b = unpinned; pin_b.set_certificate_pin(std::string(64, 'b'));
    EXPECT_NE(detail::connection_fingerprint(unpinned), detail::connection_fingerprint(pin_a));
    EXPECT_NE(detail::connection_fingerprint(pin_a), detail::connection_fingerprint(pin_b));
    auto& cache = detail::LatticeCache::instance();
    auto original = cache.get_or_create(unpinned, SchemaVector{});
    auto first = cache.get_or_create(pin_a, SchemaVector{});
    auto second = cache.get_or_create(pin_b, SchemaVector{});
    EXPECT_NE(original.get(), first.get());
    EXPECT_NE(first.get(), second.get());
    EXPECT_EQ(first.get(), cache.get_or_create(pin_a, SchemaVector{}).get());
    EXPECT_EQ(first->config().pinned_server_certificate_sha256, std::string(64, 'a'));
    second->close(); first->close(); original->close();
}
} // namespace
