#include "tcApp.h"

void tcApp::setup() {
    setIndependentFps(VSYNC, 0);

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
    setBackground(0.08f, 0.10f, 0.12f);
    setColor(0.9f, 0.9f, 0.95f);
    float y = 30;
    drawText("tcxMQTT — asyncPubSub (Event<T> listeners)", 20, y);
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
