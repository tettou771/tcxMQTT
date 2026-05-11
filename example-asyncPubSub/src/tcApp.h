#pragma once

#include <TrussC.h>
#include "tcxMQTT.h"

#include <deque>
#include <mutex>

using namespace std;
using namespace tc;
using namespace tcx;

// =============================================================================
// Async style — register EventListener members on the client's Event<T>.
// Lambdas fire from MQTTClient::update(), so it's safe to touch UI state.
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

    // Hold the listeners — when these go out of scope, listeners disconnect.
    EventListener msgListener_;
    EventListener connectListener_;
    EventListener disconnectListener_;
    EventListener errorListener_;

    // Match these to your broker
    string host_  = "localhost";
    int    port_  = 1883;
    string user_;
    string pass_;
    string topic_ = "tcxMQTT/test";

    deque<string> log_;
    static constexpr size_t MAX_LOG = 12;
    void addLog(const string& s);
};
