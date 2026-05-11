#pragma once

// =============================================================================
// tcxMQTTMessage.h - MQTT message struct
// =============================================================================

#include <string>
#include <vector>
#include <cstdint>

namespace tcx {

struct MQTTMessage {
    std::string topic;
    std::vector<uint8_t> payload;
    int qos = 0;
    bool retained = false;

    // Convenience: payload as UTF-8 string
    std::string payloadAsString() const {
        return std::string(payload.begin(), payload.end());
    }
};

} // namespace tcx
