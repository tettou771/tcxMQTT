# tcxMQTT

MQTT 3.1.1 client addon for [TrussC](https://github.com/tettou771/TrussC).
Wraps the vendored MIT-licensed [lwmqtt](https://github.com/256dpi/lwmqtt)
protocol library over TrussC's `TcpClient`, so the addon stays
platform-agnostic (Mac / Linux / Windows / iOS / Android / web — anywhere
TrussC's network layer works).

## Features

- MQTT 3.1.1 (QoS 0/1/2, retained, will, clean session)
- **Last Will & Testament** — `client.setWill(topic, payload, qos, retain)`
  for the standard online/offline presence pattern
- **Auto-reconnect** — `client.setAutoReconnect(true, retryIntervalMs)`;
  on link loss the worker thread retries `connect()` forever (or until
  `disconnect()`) and re-fires `onConnect` for each successful reattach.
  Subscriptions are not remembered — re-subscribe in your `onConnect`
  listener (see `example-asyncPubSub`).
- **Two usage patterns**, mix freely:
  - **Async**: register listeners on `Event<T>` members (`onMessage`,
    `onConnect`, `onDisconnect`, `onError`) — `EventListener` RAII
    auto-unsubscribes on destruction
  - **Sync**: poll with `hasNewMessage()` / `getNextMessage()` in your
    `update()` loop; the internal queue is allocated lazily on first
    poll, so async-only users pay no queue cost
- Username / password auth (anonymous if both empty)
- **TLS / MQTTS** via the `tcxTls` addon — see `example-tlsPubSub/`
  and the *Transport (TLS)* section below.

## Install

This addon lives under `TrussC/addons/tcxMQTT/`. Add to your TrussC app's
`addons.make`:

```
tcxMQTT
```

then `trusscli update` and `trusscli build`.

## Examples

- `example-basicPubSub/` — sync polling style, plain TCP
- `example-asyncPubSub/` — event listener style, plain TCP
- `example-tlsPubSub/` — async + MQTTS over `tcxTls`

Both examples read broker connection details from environment variables so
passwords don't end up in the repo:

```bash
cd example-asyncPubSub
TCXMQTT_HOST=broker.example.com \
TCXMQTT_PORT=1883 \
TCXMQTT_USER=alice \
TCXMQTT_PASS=s3cret \
TCXMQTT_TOPIC=tcxMQTT/test \
  trusscli run
```

Without env vars they default to `localhost:1883` — handy for running a
throwaway `mosquitto -p 1883` locally.

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

## Transport (TLS)

The default transport is plain TCP. To speak **MQTTS**, install the
[`tcxTls`](../tcxTls) addon (mbedTLS-based), build a `TlsClient`,
configure it, and hand it to `MQTTClient::setTransport()` BEFORE
`connect()`:

```cpp
#include "tcxMQTT.h"
#include "tcTlsClient.h"
using namespace tc;
using namespace tcx;

MQTTClient mqtt;

auto tls = std::make_unique<TlsClient>();
tls->setHostname("broker.example.com");        // SNI + cert verify
// tls->setCACertificateFile("/path/to/ca.pem"); // optional private CA
// tls->setVerifyNone();                          // testing only

mqtt.setTransport(std::move(tls));
mqtt.connect("broker.example.com", 8883, /*clientId*/ "", "alice", "s3cret");
```

`setTransport(nullptr)` resets back to plain TCP. The call is rejected
while connected — call `disconnect()` first if you want to switch
transports at runtime.

## License

MIT for tcxMQTT itself (this directory).
The vendored lwmqtt is also MIT — see `src/vendor/lwmqtt/LICENSE`.
