/*
 * asyncPubSub — Event<T> + EventListener demo for tcxMQTT.
 *
 * Demonstrates the listener-based API: each `Event<T>` exposed by
 * `MQTTClient` (`onConnect`, `onDisconnect`, `onMessage`, `onError`) is
 * bound via `.listen([](...) { ... })`. The returned EventListener is
 * stored as a member so it stays alive — TrussC's listener pattern is
 * RAII; when the EventListener is destroyed the lambda unsubscribes
 * automatically. Reach for this style when you want event-driven
 * dispatch instead of polling, or when you need to react to connect /
 * disconnect / error edges as well as plain messages.
 *
 * onConnect (re-)subscribes after every successful CONNACK, so a future
 * tcxMQTT auto-reconnect path will resubscribe seamlessly without app
 * changes. onMessage logs payloads. Press Space to publish back to the
 * same topic; the broker echoes it and the round-trip lights up in the
 * on-screen log.
 *
 * Broker connection details are read from env vars
 * (TCXMQTT_HOST / TCXMQTT_PORT / TCXMQTT_USER / TCXMQTT_PASS / TCXMQTT_TOPIC)
 * if set, otherwise the defaults in tcApp.h target a local mosquitto at
 * localhost:1883.
 */

#include "tcApp.h"
#include <cstdlib>

void tcApp::setup() {
    setIndependentFps(VSYNC, 0);

    // Env-var overrides so a real broker (with auth) can be exercised without
    // committing the password. Defaults in tcApp.h target a local mosquitto.
    if (const char* v = std::getenv("TCXMQTT_HOST"))  host_  = v;
    if (const char* v = std::getenv("TCXMQTT_PORT"))  port_  = std::atoi(v);
    if (const char* v = std::getenv("TCXMQTT_USER"))  user_  = v;
    if (const char* v = std::getenv("TCXMQTT_PASS"))  pass_  = v;
    if (const char* v = std::getenv("TCXMQTT_TOPIC")) topic_ = v;

    // Hook up listeners BEFORE connect, so the onConnect notify isn't missed.
    msgListener_ = mqtt_.onMessage.listen([this](MQTTMessage& m) {
        addLog("RX [" + m.topic + "] " + m.payloadAsString());
    });
    connectListener_ = mqtt_.onConnect.listen([this]() {
        addLog("event: connected");
        mqtt_.subscribe(topic_);
    });
    disconnectListener_ = mqtt_.onDisconnect.listen([this]() {
        addLog("event: disconnected");
    });
    errorListener_ = mqtt_.onError.listen([this](string& s) {
        addLog("event: error: " + s);
    });

    addLog("Connecting to " + host_ + ":" + to_string(port_) + " ...");
    mqtt_.connect(host_, port_, /*clientId*/"", user_, pass_);
}

void tcApp::update() {
    // Drains queued events; listeners fire here on the main thread.
    mqtt_.update();
}

void tcApp::draw() {
    clear(0.08f, 0.10f, 0.12f, 1.0f);
    setColor(Color(0.9f, 0.9f, 0.95f));
    float y = 30;
    drawBitmapString("tcxMQTT — asyncPubSub (Event<T> listeners)", 20, y);
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
