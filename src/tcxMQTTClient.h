#pragma once

// =============================================================================
// tcxMQTTClient.h - MQTT 3.1.1 client (lwmqtt + TrussC TcpClient)
// =============================================================================
// Two usage patterns supported:
//   - async: register listeners on the Event<T> members (RAII, see TrussC docs)
//   - sync : call hasNewMessage() / getNextMessage() in update()
// They can be mixed freely. The sync queue is allocated lazily the first
// time hasNewMessage() is called; pure async users pay no queue cost.

#include "tcxMQTTMessage.h"
#include "tc/events/tcEvent.h"
#include "tc/events/tcEventListener.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

namespace trussc { class TcpClient; }

namespace tcx {

class MQTTClient {
public:
    // -------------------------------------------------------------------------
    // Events (async pattern)
    // -------------------------------------------------------------------------
    ::trussc::Event<MQTTMessage> onMessage;       // Published message arrived
    ::trussc::Event<void>        onConnect;       // CONNACK accepted
    ::trussc::Event<void>        onDisconnect;    // Socket closed / lost
    ::trussc::Event<std::string> onError;         // Protocol or transport error

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------
    MQTTClient();
    ~MQTTClient();

    MQTTClient(const MQTTClient&) = delete;
    MQTTClient& operator=(const MQTTClient&) = delete;

    // Connect to broker. Blocks until CONNACK or timeout (default 5s).
    // clientId empty -> auto-generated. username empty -> anonymous.
    bool connect(const std::string& host, int port,
                 const std::string& clientId = "",
                 const std::string& username = "",
                 const std::string& password = "",
                 int timeoutMs = 5000);

    // Tear down connection. Safe to call multiple times.
    void disconnect();

    bool isConnected() const;

    // -------------------------------------------------------------------------
    // Pub/Sub
    // -------------------------------------------------------------------------
    bool subscribe(const std::string& topic, int qos = 0);
    bool unsubscribe(const std::string& topic);

    bool publish(const std::string& topic,
                 const std::string& payload,
                 int qos = 0, bool retain = false);

    bool publish(const std::string& topic,
                 const uint8_t* data, size_t len,
                 int qos = 0, bool retain = false);

    // -------------------------------------------------------------------------
    // Polling — must be called regularly (typically in App::update())
    // -------------------------------------------------------------------------
    // Drives keep-alive PING and dispatches queued events to the main thread.
    void update();

    // -------------------------------------------------------------------------
    // Sync polling API (buffer enabled lazily on first call)
    // -------------------------------------------------------------------------
    bool   hasNewMessage();
    bool   getNextMessage(MQTTMessage& out);
    size_t numNewMessages() const;
    // Caps BOTH the sync polling queue and the async dispatch queue at
    // `size` entries each. On overflow the oldest entry is dropped.
    // Default 1024. Set higher if your app legitimately bursts more
    // events than that between update() calls; set lower for memory-
    // constrained devices.
    void   setBufferSize(size_t size);
    size_t getBufferSize() const;

    // -------------------------------------------------------------------------
    // Options (call before connect)
    // -------------------------------------------------------------------------
    void setKeepAlive(int seconds);     // default 60
    void setCleanSession(bool clean);   // default true

    // Last Will & Testament — broker publishes `payload` to `topic` if
    // this client disconnects ungracefully. Standard MQTT presence
    // pattern. Pass before connect(); takes effect on next connect.
    void setWill(const std::string& topic, const std::string& payload,
                 int qos = 0, bool retain = false);
    void clearWill();

    // Auto-reconnect — when the link drops, retry connect() on a
    // background thread with the same credentials. Disabled by default.
    // On each successful reconnect, onConnect fires again; listeners
    // should re-subscribe there if they care (subscriptions are not
    // remembered by this client).
    void setAutoReconnect(bool enable, int retryIntervalMs = 5000);

    // -------------------------------------------------------------------------
    // Transport (advanced — for TLS / MQTTS)
    // -------------------------------------------------------------------------
    // Replace the default plain-TCP transport with a custom one.
    //
    // Typical use is MQTTS via the tcxTls addon:
    //
    //   #include "tcTlsClient.h"
    //   auto tls = std::make_unique<tc::TlsClient>();
    //   tls->setHostname("broker.example.com");        // SNI / cert verify
    //   // tls->setVerifyNone();                        // testing only
    //   mqtt.setTransport(std::move(tls));
    //   mqtt.connect("broker.example.com", 8883, ...);
    //
    // Must be called BEFORE connect() (or after disconnect()). Returns
    // false and ignores the transport if currently connected. Passing
    // nullptr resets to the default plain TcpClient.
    bool setTransport(std::unique_ptr<::trussc::TcpClient> transport);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace tcx
