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
    std::vector<uint8_t> rxBuf;
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
    std::queue<Pending> pending;
    std::mutex pendingMutex;

    // ---- sync polling queue ------------------------------------------------
    std::queue<MQTTMessage> syncQueue;
    std::mutex syncMutex;
    std::atomic<bool> bufferEnabled{false};
    size_t bufferMax = 100;

    // ---- options -----------------------------------------------------------
    int keepAliveSec = 60;
    bool cleanSession = true;

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
    void enqueueMessage(MQTTMessage&& m);
    void enqueueConnect();
    void enqueueDisconnect();
    void enqueueError(std::string s);
    void yieldLoop();
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
        if (self->rxClosed) return LWMQTT_NETWORK_FAILED_READ;

        // Drain as much as available
        size_t avail = self->rxBuf.size();
        if (avail > 0) {
            size_t take = std::min(avail, len - *read);
            std::memcpy(buf + *read, self->rxBuf.data(), take);
            self->rxBuf.erase(self->rxBuf.begin(), self->rxBuf.begin() + take);
            *read += take;
            continue;
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

void MQTTClient::Impl::enqueueMessage(MQTTMessage&& m) {
    // Sync queue (only if user has consulted it at least once)
    if (bufferEnabled) {
        std::lock_guard<std::mutex> lk(syncMutex);
        syncQueue.push(m);
        while (syncQueue.size() > bufferMax) syncQueue.pop();
    }
    // Dispatch via pending; update() drains it on the main thread.
    std::lock_guard<std::mutex> lk(pendingMutex);
    pending.push({PendingKind::Message, std::move(m), {}});
}

void MQTTClient::Impl::enqueueConnect() {
    std::lock_guard<std::mutex> lk(pendingMutex);
    pending.push({PendingKind::Connect, {}, {}});
}
void MQTTClient::Impl::enqueueDisconnect() {
    std::lock_guard<std::mutex> lk(pendingMutex);
    pending.push({PendingKind::Disconnect, {}, {}});
}
void MQTTClient::Impl::enqueueError(std::string s) {
    std::lock_guard<std::mutex> lk(pendingMutex);
    pending.push({PendingKind::Error, {}, std::move(s)});
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
    while (yieldRunning) {
        {
            std::lock_guard<std::mutex> lk(lwMutex);
            // 200ms slice — lwmqtt_yield blocks up to this long inside netRead.
            lwmqtt_err_t e = lwmqtt_yield(&lw, BUF, 200);
            if (e != LWMQTT_SUCCESS && e != LWMQTT_NETWORK_TIMEOUT) {
                enqueueError("lwmqtt_yield: " + std::to_string((int)e));
                if (connected) {
                    connected = false;
                    enqueueDisconnect();
                }
                break;
            }
            e = lwmqtt_keep_alive(&lw, 1000);
            if (e != LWMQTT_SUCCESS) {
                enqueueError("lwmqtt_keep_alive: " + std::to_string((int)e));
                if (connected) {
                    connected = false;
                    enqueueDisconnect();
                }
                break;
            }
        }
        // tiny sleep so we don't pin a core when there's no traffic
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
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

    // Wire TcpClient events. We use the synchronous send() path for outgoing,
    // and accumulate incoming bytes into rxBuf where netRead can pull them.
    impl_->tcp.setUseThread(true);
    impl_->tcpReceive = impl_->tcp.onReceive.listen(
        [this](TcpReceiveEventArgs& a) {
            std::lock_guard<std::mutex> lk(impl_->rxMutex);
            impl_->rxBuf.insert(impl_->rxBuf.end(),
                                a.data.begin(), a.data.end());
            impl_->rxCv.notify_all();
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
    impl_->resetState();

    if (!impl_->tcp.connect(host, port)) {
        impl_->enqueueError("tcp.connect failed");
        return false;
    }
    // Wait for TCP to actually be up. TcpClient's connect is typically
    // synchronous from the caller's perspective if useThread=false; with
    // useThread=true we briefly wait for the onConnect notify or a timeout.
    auto deadline = std::chrono::steady_clock::now()
                  + std::chrono::milliseconds(timeoutMs);
    while (!impl_->tcp.isConnected()
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!impl_->tcp.isConnected()) {
        impl_->enqueueError("tcp connect timeout");
        impl_->tcp.disconnect();
        return false;
    }

    // Build CONNECT options
    lwmqtt_connect_options_t opts = lwmqtt_default_connect_options;
    std::string cid = clientId;
    if (cid.empty()) {
        // Generate a small random suffix
        std::random_device rd;
        char tmp[24];
        std::snprintf(tmp, sizeof(tmp), "tcxmqtt-%08x", (unsigned)rd());
        cid = tmp;
    }
    opts.client_id   = lwmqtt_string(cid.c_str());
    opts.keep_alive  = (uint16_t)impl_->keepAliveSec;
    opts.clean_session = impl_->cleanSession;

    lwmqtt_string_t userStr = lwmqtt_default_string;
    lwmqtt_string_t passStr = lwmqtt_default_string;
    if (!username.empty()) {
        userStr = lwmqtt_string(username.c_str());
        opts.username = userStr;
        if (!password.empty()) {
            passStr = lwmqtt_string(password.c_str());
            opts.password = passStr;
        }
    }

    lwmqtt_err_t e;
    {
        std::lock_guard<std::mutex> lk(impl_->lwMutex);
        e = lwmqtt_connect(&impl_->lw, &opts, nullptr, timeoutMs);
    }
    lwmqtt_return_code_t rc = opts.return_code;
    if (e != LWMQTT_SUCCESS || rc != LWMQTT_CONNECTION_ACCEPTED) {
        impl_->enqueueError("CONNECT failed err=" + std::to_string((int)e)
                            + " rc=" + std::to_string((int)rc));
        impl_->tcp.disconnect();
        return false;
    }

    impl_->connected = true;
    impl_->yieldRunning = true;
    impl_->yieldThread = std::thread([this]() { impl_->yieldLoop(); });
    impl_->enqueueConnect();
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

} // namespace tcx
