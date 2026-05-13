/*
 * basicPubSub — sync polling demo for tcxMQTT.
 *
 * Connects to a broker on setup(), subscribes to one topic, then in every
 * update() drains whatever has arrived with `MQTTClient::getNextMessage()`.
 * No listeners, no RAII gotchas — just `hasNewMessage()` / `getNextMessage()`
 * mirroring tcxOscReceiver's polling API. Use this style when you'd rather
 * keep message handling inline in your update loop than reason about
 * listener lifetimes.
 *
 * Press Space to publish a "ping@<seconds>" payload back to the same
 * topic. Because we're also subscribed, the broker echoes it to us and
 * the round-trip is visible in the on-screen log.
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

    addLog("Connecting to " + host_ + ":" + to_string(port_) + " ...");
    if (mqtt_.connect(host_, port_, /*clientId*/"", user_, pass_)) {
        addLog("Connected. Subscribing to: " + topic_);
        mqtt_.subscribe(topic_);
    } else {
        addLog("CONNECT failed");
    }
}

void tcApp::update() {
    // Drive lwmqtt event dispatch on the main thread (no listeners here,
    // but cheap and harmless).
    mqtt_.update();

    // Pull every queued message synchronously.
    MQTTMessage m;
    while (mqtt_.getNextMessage(m)) {
        addLog("RX [" + m.topic + "] " + m.payloadAsString());
    }
}

void tcApp::draw() {
    clear(0.08f, 0.10f, 0.12f, 1.0f);
    setColor(Color(0.9f, 0.9f, 0.95f));
    float y = 30;
    drawBitmapString("tcxMQTT — basicPubSub (sync polling)", 20, y);
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
