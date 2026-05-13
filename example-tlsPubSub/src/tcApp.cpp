/*
 * tlsPubSub — MQTTS over tcxTls.
 *
 * Mirrors example-asyncPubSub line-for-line on the MQTT side; the only
 * differences are:
 *   1. addons.make also pulls in tcxTls
 *   2. We construct a tc::TlsClient, configure it (hostname for cert
 *      verification, optional CA file, optional verify-none for testing)
 *      and pass it to MQTTClient::setTransport() BEFORE connect().
 *
 * Defaults target test.mosquitto.org:8883 (anonymous, no auth). For your
 * own broker, set TCXMQTT_HOST/PORT/USER/PASS/TOPIC env vars. To trust a
 * private CA, point TCXMQTT_CA_FILE at a PEM bundle. To skip verification
 * entirely (lab/dev only — disables MITM protection), set TCXMQTT_INSECURE=1.
 */

#include "tcApp.h"
#include <cstdlib>
#include <memory>

void tcApp::setup() {
    setIndependentFps(VSYNC, 0);

    if (const char* v = std::getenv("TCXMQTT_HOST"))     host_       = v;
    if (const char* v = std::getenv("TCXMQTT_PORT"))     port_       = std::atoi(v);
    if (const char* v = std::getenv("TCXMQTT_USER"))     user_       = v;
    if (const char* v = std::getenv("TCXMQTT_PASS"))     pass_       = v;
    if (const char* v = std::getenv("TCXMQTT_TOPIC"))    topic_      = v;
    if (const char* v = std::getenv("TCXMQTT_CA_FILE"))  caFile_     = v;
    if (const char* v = std::getenv("TCXMQTT_INSECURE")) verifyNone_ = (v[0] == '1');

    msgListener_ = mqtt_.onMessage.listen([this](MQTTMessage& m) {
        addLog("RX [" + m.topic + "] " + m.payloadAsString());
    });
    connectListener_ = mqtt_.onConnect.listen([this]() {
        addLog("event: connected (TLS)");
        mqtt_.subscribe(topic_);
    });
    disconnectListener_ = mqtt_.onDisconnect.listen([this]() {
        addLog("event: disconnected");
    });
    errorListener_ = mqtt_.onError.listen([this](string& s) {
        addLog("event: error: " + s);
    });

    // Build & install the TLS transport. Order matters:
    //   (1) configure TlsClient   (2) setTransport   (3) connect
    auto tls = std::make_unique<TlsClient>();
    tls->setHostname(host_);     // SNI + cert hostname check
    if (verifyNone_) {
        tls->setVerifyNone();    // testing only
        addLog("TLS: cert verification DISABLED");
    } else if (!caFile_.empty()) {
        if (tls->setCACertificateFile(caFile_)) {
            addLog("TLS: trusting CA from " + caFile_);
        } else {
            addLog("TLS: failed to load CA " + caFile_);
        }
    }
    if (!mqtt_.setTransport(std::move(tls))) {
        addLog("setTransport rejected (already connected?)");
        return;
    }

    addLog("Connecting to " + host_ + ":" + to_string(port_) + " (TLS) ...");
    mqtt_.connect(host_, port_, /*clientId*/"", user_, pass_);
}

void tcApp::update() {
    mqtt_.update();
}

void tcApp::draw() {
    clear(0.08f, 0.10f, 0.12f, 1.0f);
    setColor(Color(0.9f, 0.9f, 0.95f));
    float y = 30;
    drawBitmapString("tcxMQTT — tlsPubSub (MQTTS via tcxTls)", 20, y);
    y += 24;
    drawBitmapString("Space = publish \"ping\" to " + topic_, 20, y);
    y += 18;
    drawBitmapString("Status: " + string(mqtt_.isConnected() ? "CONNECTED" : "DISCONNECTED"),
             20, y);
    y += 26;
    for (auto& line : log_) {
        drawBitmapString(line, 20, y);
        y += 16;
    }
}

void tcApp::cleanup() {
    mqtt_.disconnect();
}

void tcApp::keyPressed(int key) {
    if (key == ' ') {
        const string payload = "ping@" + to_string((int)getElapsedTimef());
        if (mqtt_.publish(topic_, payload)) {
            addLog("TX [" + topic_ + "] " + payload);
        } else {
            addLog("publish failed");
        }
    }
}

void tcApp::addLog(const string& s) {
    log_.push_back(s);
    while (log_.size() > MAX_LOG) log_.pop_front();
    redraw();
}
