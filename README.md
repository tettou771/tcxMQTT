# tcxMQTT

MQTT 3.1.1 client addon for [TrussC](https://github.com/tettou771/TrussC).
Wraps the vendored MIT-licensed [lwmqtt](https://github.com/256dpi/lwmqtt)
protocol library over TrussC's `TcpClient`, so the addon stays
platform-agnostic (Mac / Linux / Windows / iOS / Android / web — anywhere
TrussC's network layer works).

## Features

- MQTT 3.1.1 (QoS 0/1/2, retained, will, clean session)
- **Two usage patterns**, mix freely:
  - **Async**: register listeners on `Event<T>` members (`onMessage`,
    `onConnect`, `onDisconnect`, `onError`) — `EventListener` RAII
    auto-unsubscribes on destruction
  - **Sync**: poll with `hasNewMessage()` / `getNextMessage()` in your
    `update()` loop; the internal queue is allocated lazily on first
    poll, so async-only users pay no queue cost
- Username / password auth (anonymous if both empty)
- Plain TCP today; TLS is on the roadmap (would route through
  TrussC's `TlsClient` instead of `TcpClient`)

## Install

This addon lives under `TrussC/addons/tcxMQTT/`. Add to your TrussC app's
`addons.make`:

```
tcxMQTT
```

then `trusscli update` and `trusscli build`.

## Examples

See `example-basicPubSub/` (sync polling style) and `example-asyncPubSub/`
(event listener style) in this addon directory.

## API sketch

```cpp
#include "tcxMQTT.h"
using namespace tcx;

class tcApp : public App {
    MQTTClient mqtt;
    EventListener msgListener;   // hold this — RAII unsubscribes on destruction

    void setup() override {
        msgListener = mqtt.onMessage.listen([](MQTTMessage& m){
            logNotice("MQTT") << m.topic << " -> "
                              << m.payloadAsString();
        });
        mqtt.connect("broker.example.com", 1883,
                     /*clientId*/ "", /*user*/ "alice", /*pass*/ "s3cret");
        mqtt.subscribe("hello/world");
    }

    void update() override {
        mqtt.update();           // dispatches queued events on this thread
        // or pull synchronously:
        MQTTMessage m;
        while (mqtt.getNextMessage(m)) {
            // ...
        }
    }
};
```

## License

MIT for tcxMQTT itself (this directory).
The vendored lwmqtt is also MIT — see `src/vendor/lwmqtt/LICENSE`.
