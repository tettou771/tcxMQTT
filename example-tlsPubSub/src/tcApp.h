#pragma once

#include <TrussC.h>
#include "tcxMQTT.h"
#include "tcTlsClient.h"

#include <deque>

using namespace std;
using namespace tc;
using namespace tcx;

// =============================================================================
// tlsPubSub — MQTTS demo for tcxMQTT.
//
// Same async-listener pattern as example-asyncPubSub, but the transport
// is swapped to a tc::TlsClient via mqtt_.setTransport(). The TlsClient
// is configured BEFORE setTransport() — hostname for SNI / cert verify,
// optional CA setup, and (for testing only) setVerifyNone().
// =============================================================================
class tcApp : public App {
public:
    void setup() override;
    void update() override;
    void draw() override;
    void cleanup() override;
    void keyPressed(int key) override;

private:
    MQTTClient mqtt_;

    EventListener msgListener_;
    EventListener connectListener_;
    EventListener disconnectListener_;
    EventListener errorListener_;

    // Defaults target test.mosquitto.org on its TLS port (8883). Override
    // any of these via env vars at run time — see setup().
    string host_  = "test.mosquitto.org";
    int    port_  = 8883;
    string user_;
    string pass_;
    string topic_ = "tcxMQTT/test";
    string caFile_;     // PEM bundle path; empty = use default trust store.
    bool   verifyNone_ = false;  // true skips cert verification (testing only).

    deque<string> log_;
    static constexpr size_t MAX_LOG = 12;
    void addLog(const string& s);
};
