#include "tcApp.h"

void tcApp::setup() {
    setIndependentFps(VSYNC, 0);
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
    setBackground(0.08f, 0.10f, 0.12f);
    setColor(0.9f, 0.9f, 0.95f);
    float y = 30;
    drawText("tcxMQTT — basicPubSub (sync polling)", 20, y);
    y += 24;
    drawText("Space = publish \"ping\" to " + topic_, 20, y);
    y += 18;
    drawText("Status: " + string(mqtt_.isConnected() ? "CONNECTED" : "DISCONNECTED"),
             20, y);
    y += 26;
    for (auto& line : log_) {
        drawText(line, 20, y);
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
