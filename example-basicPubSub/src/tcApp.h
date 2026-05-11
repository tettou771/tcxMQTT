#pragma once

#include <TrussC.h>
#include "tcxMQTT.h"

#include <deque>
#include <mutex>

using namespace std;
using namespace tc;
using namespace tcx;

// =============================================================================
// Sync polling style — call hasNewMessage() / getNextMessage() each frame.
// Space publishes a "ping" to the same topic so you can see it round-trip.
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

    // Match these to your broker
    string host_    = "localhost";
    int    port_    = 1883;
    string user_;
    string pass_;
    string topic_   = "tcxMQTT/test";

    deque<string> log_;
    static constexpr size_t MAX_LOG = 12;

    void addLog(const string& s);
};
