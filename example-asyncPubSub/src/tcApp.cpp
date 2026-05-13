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
