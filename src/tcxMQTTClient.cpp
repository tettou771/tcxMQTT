// =============================================================================
// tcxMQTTClient.cpp - implementation (lwmqtt + TrussC TcpClient bridge)
// =============================================================================

#include "tcxMQTTClient.h"

#include "tc/network/tcTcpClient.h"
#include "tc/utils/tcLog.h"

extern "C" {
#include "lwmqtt.h"
}

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <mutex>
#include <queue>
#include <random>
#include <thread>
#include <vector>

using namespace ::trussc;

namespace tcx {

// =============================================================================
// Pimpl
// =============================================================================
struct MQTTClient::Impl {
    MQTTClient* owner = nullptr;

    // ---- lwmqtt state ------------------------------------------------------
    lwmqtt_client_t lw;
    static constexpr size_t BUF = 2048;
    uint8_t writeBuf[BUF];
    uint8_t readBuf[BUF];

    // lwmqtt timer callbacks need a per-timer reference; we use two ms-since-set
    // epochs (keep-alive timer + command timer).
    int64_t keepAliveDeadline = 0;
    int64_t commandDeadline = 0;

    // ---- TCP transport -----------------------------------------------------
    TcpClient tcp;
    EventListener tcpConnect;
    EventListener tcpReceive;
    EventListener tcpDisconnect;
    EventListener tcpError;

    std::mutex rxMutex;
    std::condition_variable rxCv;
    // Bounded inbound buffer. A misbehaving broker that flooded data faster
    // than the yield thread could consume would otherwise grow rxBuf without
    // limit and OOM the process. deque is used (not vector) so the front-
    // erase in netRead is O(1) per chunk, not O(n^2) over the buffer.
    std::deque<uint8_t> rxBuf;
    static constexpr size_t RX_BUF_MAX = 1024 * 1024;  // 1 MiB
    std::atomic<bool> rxClosed{false};

    // ---- yield worker ------------------------------------------------------
    std::thread yieldThread;
    std::atomic<bool> yieldRunning{false};

    // ---- pending dispatches (yield thread -> update()) ---------------------
    // Events are notified from update() on the main thread to keep listener
    // bodies on the main thread (drawing-friendly).
    enum class PendingKind { Message, Connect, Disconnect, Error };
    struct Pending {
        PendingKind kind;
        MQTTMessage msg;
        std::string err;
    };
    // Bounded pending queue. Without a cap a flood of broker PUBLISHes
    // delivered while the app's update() is slow would grow this queue
    // unboundedly. When full we drop the oldest message and emit at most
    // one error notification so the user can react.
    std::queue<Pending> pending;
    std::mutex pendingMutex;
    static constexpr size_t PENDING_MAX = 1024;
    bool pendingOverflowReported = false;

    // ---- sync polling queue ------------------------------------------------
    std::queue<MQTTMessage> syncQueue;
    std::mutex syncMutex;
    std::atomic<bool> bufferEnabled{false};
    size_t bufferMax = 100;

    // ---- options -----------------------------------------------------------
    int keepAliveSec = 60;
    bool cleanSession = true;

    // ---- Last Will & Testament --------------------------------------------
    bool        willSet = false;
    std::string willTopic;
    std::string willPayload;
    int         willQos = 0;
    bool        willRetain = false;

    // ---- auto-reconnect ----------------------------------------------------
    std::atomic<bool> autoReconnect{false};
    std::atomic<int>  retryIntervalMs{5000};
    // Remembered for reconnect (last successful connect()'s args).
    std::string lastHost;
    int         lastPort = 0;
    std::string lastClientId;
    std::string lastUser;
    std::string lastPass;
    int         lastTimeoutMs = 5000;

    // ---- state -------------------------------------------------------------
    std::atomic<bool> connected{false};
    std::mutex lwMutex;  // serialize lwmqtt_* calls

    // ---- lwmqtt callbacks --------------------------------------------------
    static lwmqtt_err_t netRead(void* ref, uint8_t* buf, size_t len, size_t* read, uint32_t timeoutMs);
    static lwmqtt_err_t netWrite(void* ref, uint8_t* buf, size_t len, size_t* sent, uint32_t timeoutMs);
    static void timerSet(void* ref, uint32_t timeoutMs);
    static int32_t timerGet(void* ref);
    static void msgCb(lwmqtt_client_t* client, void* ref,
                      lwmqtt_string_t topic, lwmqtt_message_t message);

    // ---- helpers -----------------------------------------------------------
    void resetState();
    void pushPending(Pending&& p);
    void enqueueMessage(MQTTMessage&& m);
    void enqueueConnect();
    void enqueueDisconnect();
    void enqueueError(std::string s);
    void yieldLoop();
    bool tryConnectInternal();   // TCP + MQTT CONNECT using saved credentials
};

// =============================================================================
// network read / write (called from yield thread on lwmqtt's behalf)
// =============================================================================

lwmqtt_err_t MQTTClient::Impl::netRead(void* ref, uint8_t* buf, size_t len,
                                        size_t* read, uint32_t timeoutMs) {
    auto* self = static_cast<Impl*>(ref);
    *read = 0;

    auto deadline = std::chrono::steady_clock::now()
                  + std::chrono::milliseconds(timeoutMs);

    std::unique_lock<std::mutex> lk(self->rxMutex);
    while (*read < len) {
        // Drain rxBuf BEFORE checking rxClosed: a peer that sends the
        // last bytes of a packet then immediately FINs would otherwise
        // see those bytes dropped if the close notification beat us here.
        size_t avail = self->rxBuf.size();
        if (avail > 0) {
            size_t take = std::min(avail, len - *read);
            for (size_t i = 0; i < take; i++) {
                buf[*read + i] = self->rxBuf.front();
                self->rxBuf.pop_front();
            }
            *read += take;
            continue;
        }
        if (self->rxClosed) {
            return *read > 0 ? LWMQTT_SUCCESS : LWMQTT_NETWORK_FAILED_READ;
        }

        // Wait for more data
        if (self->rxCv.wait_until(lk, deadline) == std::cv_status::timeout) {
            return *read > 0 ? LWMQTT_SUCCESS : LWMQTT_NETWORK_TIMEOUT;
        }
    }
    return LWMQTT_SUCCESS;
}

lwmqtt_err_t MQTTClient::Impl::netWrite(void* ref, uint8_t* buf, size_t len,
                                         size_t* sent, uint32_t /*timeoutMs*/) {
    auto* self = static_cast<Impl*>(ref);
    if (self->rxClosed) {
        *sent = 0;
        return LWMQTT_NETWORK_FAILED_WRITE;
    }
    // TrussC TcpClient::send is best-effort synchronous (writes to OS socket).
    // We treat the whole buffer as written if send returns true.
    bool ok = self->tcp.send(reinterpret_cast<const char*>(buf), len);
    *sent = ok ? len : 0;
    return ok ? LWMQTT_SUCCESS : LWMQTT_NETWORK_FAILED_WRITE;
}

// =============================================================================
// timers (monotonic ms; lwmqtt only needs relative)
// =============================================================================

static int64_t nowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        steady_clock::now().time_since_epoch()).count();
}

void MQTTClient::Impl::timerSet(void* ref, uint32_t timeoutMs) {
    *static_cast<int64_t*>(ref) = nowMs() + (int64_t)timeoutMs;
}

int32_t MQTTClient::Impl::timerGet(void* ref) {
    int64_t remain = *static_cast<int64_t*>(ref) - nowMs();
    if (remain < INT32_MIN) remain = INT32_MIN;
    if (remain > INT32_MAX) remain = INT32_MAX;
    return (int32_t)remain;
}

// =============================================================================
// incoming PUBLISH callback (from lwmqtt_yield while holding lwMutex)
// =============================================================================

void MQTTClient::Impl::msgCb(lwmqtt_client_t* /*client*/, void* ref,
                              lwmqtt_string_t topic, lwmqtt_message_t message) {
    auto* self = static_cast<Impl*>(ref);
    MQTTMessage m;
    m.topic.assign(topic.data, topic.len);
    m.payload.assign(message.payload, message.payload + message.payload_len);
    m.qos = (int)message.qos;
    m.retained = message.retained;
    self->enqueueMessage(std::move(m));
}

// =============================================================================
// helpers
// =============================================================================

// Push a Pending onto the dispatch queue, bounded by PENDING_MAX. On
// overflow the OLDEST entry is dropped (so a slow app sees the most
// recent state) and a single overflow-error notice is queued once until
// drained. Must be called with pendingMutex NOT held.
void MQTTClient::Impl::pushPending(Pending&& p) {
    std::lock_guard<std::mutex> lk(pendingMutex);
    if (pending.size() >= PENDING_MAX) {
        pending.pop();
        if (!pendingOverflowReported) {
            pendingOverflowReported = true;
            pending.push({PendingKind::Error, {},
                          "pending queue overflow — events dropped"});
        }
    } else {
        // Reset the one-shot overflow flag once we have headroom again.
        if (pending.size() < PENDING_MAX / 2) pendingOverflowReported = false;
    }
    pending.push(std::move(p));
}

void MQTTClient::Impl::enqueueMessage(MQTTMessage&& m) {
    // Sync queue (only if user has consulted it at least once). This one
    // is already bounded by bufferMax (default 100).
    if (bufferEnabled) {
        std::lock_guard<std::mutex> lk(syncMutex);
        syncQueue.push(m);
        while (syncQueue.size() > bufferMax) syncQueue.pop();
    }
    pushPending({PendingKind::Message, std::move(m), {}});
}

void MQTTClient::Impl::enqueueConnect()    { pushPending({PendingKind::Connect,    {}, {}}); }
void MQTTClient::Impl::enqueueDisconnect() { pushPending({PendingKind::Disconnect, {}, {}}); }
void MQTTClient::Impl::enqueueError(std::string s) {
    pushPending({PendingKind::Error, {}, std::move(s)});
}

void MQTTClient::Impl::resetState() {
    {
        std::lock_guard<std::mutex> lk(rxMutex);
        rxBuf.clear();
        rxClosed = false;
    }
    connected = false;
}

// =============================================================================
// yield thread — keeps lwmqtt's state machine moving even when the app's
// update() is slow. Handles incoming PUBLISH packets and PINGREQ generation.
// =============================================================================

void MQTTClient::Impl::yieldLoop() {
    // Outer loop alternates between "session live (yield + keep_alive)" and
    // "session lost (optional auto-reconnect)". On graceful disconnect()
    // yieldRunning flips to false and we exit cleanly.
    while (yieldRunning) {
        // --- session live: drive lwmqtt until something breaks ---
        bool sessionLive = true;
        while (yieldRunning && sessionLive) {
            {
                std::lock_guard<std::mutex> lk(lwMutex);
                // 200ms slice — lwmqtt_yield blocks up to this long inside netRead.
                lwmqtt_err_t e = lwmqtt_yield(&lw, BUF, 200);
                if (e != LWMQTT_SUCCESS && e != LWMQTT_NETWORK_TIMEOUT) {
                    enqueueError("lwmqtt_yield: " + std::to_string((int)e));
                    sessionLive = false;
                } else {
                    e = lwmqtt_keep_alive(&lw, 1000);
                    if (e != LWMQTT_SUCCESS) {
                        enqueueError("lwmqtt_keep_alive: " + std::to_string((int)e));
                        sessionLive = false;
                    }
                }
            }
            if (sessionLive) {
                // tiny sleep so we don't pin a core when there's no traffic
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
        }

        // --- session dropped ---
        if (connected) {
            connected = false;
            enqueueDisconnect();
        }
        tcp.disconnect();
        {
            std::lock_guard<std::mutex> lk(rxMutex);
            rxClosed = true;
            rxCv.notify_all();
            rxBuf.clear();
        }

        if (!autoReconnect) break;  // exit thread, stay disconnected

        // --- reconnect loop: retry forever (or until disconnect() / autoReconnect=false) ---
        while (yieldRunning && autoReconnect && !connected) {
            std::this_thread::sleep_for(std::chrono::milliseconds(retryIntervalMs.load()));
            if (!yieldRunning || !autoReconnect) break;
            if (tryConnectInternal()) break;  // success -> back to outer loop
        }
    }
}

// Used both by MQTTClient::connect() (initial connect) and by yieldLoop()
// (reconnect path). All connection parameters come from the saved last*
// fields; the caller is responsible for setting them before invoking us.
bool MQTTClient::Impl::tryConnectInternal() {
    if (connected) return true;
    {
        std::lock_guard<std::mutex> lk(rxMutex);
        rxBuf.clear();
        rxClosed = false;
    }

    if (!tcp.connect(lastHost, lastPort)) {
        enqueueError("tcp.connect failed");
        return false;
    }
    // Wait briefly for the TCP layer to actually come up.
    auto deadline = std::chrono::steady_clock::now()
                  + std::chrono::milliseconds(lastTimeoutMs);
    while (!tcp.isConnected()
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!tcp.isConnected()) {
        enqueueError("tcp connect timeout");
        tcp.disconnect();
        return false;
    }

    // Build CONNECT options
    lwmqtt_connect_options_t opts = lwmqtt_default_connect_options;
    std::string cid = lastClientId;
    if (cid.empty()) {
        std::random_device rd;
        char tmp[24];
        std::snprintf(tmp, sizeof(tmp), "tcxmqtt-%08x", (unsigned)rd());
        cid = tmp;
    }
    opts.client_id     = lwmqtt_string(cid.c_str());
    opts.keep_alive    = (uint16_t)keepAliveSec;
    opts.clean_session = cleanSession;

    if (!lastUser.empty()) {
        opts.username = lwmqtt_string(lastUser.c_str());
        if (!lastPass.empty()) {
            opts.password = lwmqtt_string(lastPass.c_str());
        }
    }

    // Last Will & Testament (optional). Build lwmqtt_string_t fields by
    // hand instead of via lwmqtt_string(c_str()) — the latter uses strlen,
    // which would silently truncate a binary payload at the first NUL.
    // MQTT 3.1.1 will payloads are arbitrary bytes per spec.
    lwmqtt_will_t will = lwmqtt_default_will;
    lwmqtt_will_t* willPtr = nullptr;
    if (willSet
        && willTopic.size()   <= UINT16_MAX
        && willPayload.size() <= UINT16_MAX) {
        will.topic.data    = const_cast<char*>(willTopic.data());
        will.topic.len     = (uint16_t)willTopic.size();
        will.qos           = (lwmqtt_qos_t)willQos;
        will.retained      = willRetain;
        will.payload.data  = const_cast<char*>(willPayload.data());
        will.payload.len   = (uint16_t)willPayload.size();
        willPtr = &will;
    }

    lwmqtt_err_t e;
    {
        std::lock_guard<std::mutex> lk(lwMutex);
        e = lwmqtt_connect(&lw, &opts, willPtr, lastTimeoutMs);
    }
    lwmqtt_return_code_t rc = opts.return_code;
    if (e != LWMQTT_SUCCESS || rc != LWMQTT_CONNECTION_ACCEPTED) {
        enqueueError("CONNECT failed err=" + std::to_string((int)e)
                     + " rc=" + std::to_string((int)rc));
        tcp.disconnect();
        return false;
    }

    connected = true;
    enqueueConnect();
    return true;
}

// =============================================================================
// MQTTClient public methods
// =============================================================================

MQTTClient::MQTTClient() : impl_(std::make_unique<Impl>()) {
    impl_->owner = this;
    lwmqtt_init(&impl_->lw,
                impl_->writeBuf, Impl::BUF,
                impl_->readBuf,  Impl::BUF);
    lwmqtt_set_network(&impl_->lw, impl_.get(), &Impl::netRead, &Impl::netWrite);
    lwmqtt_set_timers(&impl_->lw,
                      &impl_->keepAliveDeadline, &impl_->commandDeadline,
                      &Impl::timerSet, &Impl::timerGet);
    lwmqtt_set_callback(&impl_->lw, impl_.get(), &Impl::msgCb);

    // Drop incoming packets larger than the read buffer instead of tearing
    // down the session. Without this, a single >2KB PUBLISH (e.g. a
    // retained large message on a subscribed topic) would disconnect the
    // client and, with auto-reconnect on, loop forever.
    lwmqtt_drop_overflow(&impl_->lw, true, nullptr);

    // Wire TcpClient events. We use the synchronous send() path for outgoing,
    // and accumulate incoming bytes into rxBuf where netRead can pull them.
    impl_->tcp.setUseThread(true);
    impl_->tcpReceive = impl_->tcp.onReceive.listen(
        [this](TcpReceiveEventArgs& a) {
            bool overflow = false;
            {
                std::lock_guard<std::mutex> lk(impl_->rxMutex);
                if (impl_->rxBuf.size() + a.data.size() > Impl::RX_BUF_MAX) {
                    overflow = true;
                } else {
                    impl_->rxBuf.insert(impl_->rxBuf.end(),
                                        a.data.begin(), a.data.end());
                    impl_->rxCv.notify_all();
                }
            }
            if (overflow) {
                // Broker is sending faster than we can drain, or sending a
                // packet larger than the cap. Tear down to fail fast rather
                // than OOM. The yield thread will see rxClosed and exit.
                impl_->enqueueError("rx buffer overflow (>1MiB queued)");
                std::lock_guard<std::mutex> lk(impl_->rxMutex);
                impl_->rxClosed = true;
                impl_->rxCv.notify_all();
            }
        });
    impl_->tcpDisconnect = impl_->tcp.onDisconnect.listen(
        [this](TcpDisconnectEventArgs& /*a*/) {
            {
                std::lock_guard<std::mutex> lk(impl_->rxMutex);
                impl_->rxClosed = true;
                impl_->rxCv.notify_all();
            }
            if (impl_->connected) {
                impl_->connected = false;
                impl_->enqueueDisconnect();
            }
        });
    impl_->tcpError = impl_->tcp.onError.listen(
        [this](TcpErrorEventArgs& a) {
            impl_->enqueueError("tcp: " + a.message);
        });
}

MQTTClient::~MQTTClient() {
    disconnect();
}

bool MQTTClient::connect(const std::string& host, int port,
                          const std::string& clientId,
                          const std::string& username,
                          const std::string& password,
                          int timeoutMs) {
    if (impl_->connected) return true;

    // Remember the credentials so the auto-reconnect path in yieldLoop()
    // can rebuild the same session without the caller having to redrive it.
    impl_->lastHost      = host;
    impl_->lastPort      = port;
    impl_->lastClientId  = clientId;
    impl_->lastUser      = username;
    impl_->lastPass      = password;
    impl_->lastTimeoutMs = timeoutMs;

    if (!impl_->tryConnectInternal()) return false;

    // Spawn the yield/reconnect worker exactly once.
    if (!impl_->yieldRunning) {
        impl_->yieldRunning = true;
        impl_->yieldThread = std::thread([this]() { impl_->yieldLoop(); });
    }
    return true;
}

void MQTTClient::disconnect() {
    if (impl_->yieldRunning) {
        impl_->yieldRunning = false;
        if (impl_->yieldThread.joinable()) impl_->yieldThread.join();
    }
    if (impl_->connected) {
        std::lock_guard<std::mutex> lk(impl_->lwMutex);
        lwmqtt_disconnect(&impl_->lw, 200);
        impl_->connected = false;
    }
    impl_->tcp.disconnect();
    {
        std::lock_guard<std::mutex> lk(impl_->rxMutex);
        impl_->rxClosed = true;
        impl_->rxCv.notify_all();
    }
}

bool MQTTClient::isConnected() const {
    return impl_->connected && impl_->tcp.isConnected();
}

bool MQTTClient::subscribe(const std::string& topic, int qos) {
    if (!impl_->connected) return false;
    if (qos < 0 || qos > 2) return false;
    lwmqtt_string_t t = lwmqtt_string(topic.c_str());
    lwmqtt_qos_t q = (lwmqtt_qos_t)qos;
    std::lock_guard<std::mutex> lk(impl_->lwMutex);
    return lwmqtt_subscribe(&impl_->lw, 1, &t, &q, 2000) == LWMQTT_SUCCESS;
}

bool MQTTClient::unsubscribe(const std::string& topic) {
    if (!impl_->connected) return false;
    lwmqtt_string_t t = lwmqtt_string(topic.c_str());
    std::lock_guard<std::mutex> lk(impl_->lwMutex);
    return lwmqtt_unsubscribe(&impl_->lw, 1, &t, 2000) == LWMQTT_SUCCESS;
}

bool MQTTClient::publish(const std::string& topic, const std::string& payload,
                         int qos, bool retain) {
    return publish(topic,
                   reinterpret_cast<const uint8_t*>(payload.data()),
                   payload.size(), qos, retain);
}

bool MQTTClient::publish(const std::string& topic,
                         const uint8_t* data, size_t len,
                         int qos, bool retain) {
    if (!impl_->connected) return false;
    if (qos < 0 || qos > 2) return false;
    lwmqtt_string_t t = lwmqtt_string(topic.c_str());
    lwmqtt_message_t m = lwmqtt_default_message;
    m.qos      = (lwmqtt_qos_t)qos;
    m.retained = retain;
    m.payload  = const_cast<uint8_t*>(data);
    m.payload_len = len;
    lwmqtt_publish_options_t pubOpts = lwmqtt_default_publish_options;
    std::lock_guard<std::mutex> lk(impl_->lwMutex);
    return lwmqtt_publish(&impl_->lw, &pubOpts, t, m, 2000) == LWMQTT_SUCCESS;
}

void MQTTClient::update() {
    // Drain pending events to the main thread.
    std::queue<Impl::Pending> local;
    {
        std::lock_guard<std::mutex> lk(impl_->pendingMutex);
        std::swap(local, impl_->pending);
    }
    while (!local.empty()) {
        auto& p = local.front();
        switch (p.kind) {
            case Impl::PendingKind::Message:
                onMessage.notify(p.msg);
                break;
            case Impl::PendingKind::Connect:
                onConnect.notify();
                break;
            case Impl::PendingKind::Disconnect:
                onDisconnect.notify();
                break;
            case Impl::PendingKind::Error:
                onError.notify(p.err);
                break;
        }
        local.pop();
    }
}

// ---- sync polling --------------------------------------------------------

bool MQTTClient::hasNewMessage() {
    impl_->bufferEnabled = true;
    std::lock_guard<std::mutex> lk(impl_->syncMutex);
    return !impl_->syncQueue.empty();
}

bool MQTTClient::getNextMessage(MQTTMessage& out) {
    impl_->bufferEnabled = true;
    std::lock_guard<std::mutex> lk(impl_->syncMutex);
    if (impl_->syncQueue.empty()) return false;
    out = std::move(impl_->syncQueue.front());
    impl_->syncQueue.pop();
    return true;
}

size_t MQTTClient::numNewMessages() const {
    std::lock_guard<std::mutex> lk(impl_->syncMutex);
    return impl_->syncQueue.size();
}

void MQTTClient::setBufferSize(size_t size) {
    std::lock_guard<std::mutex> lk(impl_->syncMutex);
    impl_->bufferMax = size;
    while (impl_->syncQueue.size() > impl_->bufferMax) impl_->syncQueue.pop();
}

size_t MQTTClient::getBufferSize() const { return impl_->bufferMax; }

void MQTTClient::setKeepAlive(int seconds) { impl_->keepAliveSec = seconds; }
void MQTTClient::setCleanSession(bool clean) { impl_->cleanSession = clean; }

void MQTTClient::setWill(const std::string& topic, const std::string& payload,
                         int qos, bool retain) {
    if (qos < 0 || qos > 2) qos = 0;
    impl_->willSet     = true;
    impl_->willTopic   = topic;
    impl_->willPayload = payload;
    impl_->willQos     = qos;
    impl_->willRetain  = retain;
}

void MQTTClient::clearWill() {
    impl_->willSet = false;
    impl_->willTopic.clear();
    impl_->willPayload.clear();
}

void MQTTClient::setAutoReconnect(bool enable, int retryIntervalMs) {
    impl_->autoReconnect    = enable;
    impl_->retryIntervalMs  = retryIntervalMs > 0 ? retryIntervalMs : 5000;
}

} // namespace tcx
