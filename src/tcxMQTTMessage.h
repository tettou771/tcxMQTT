#pragma once

// =============================================================================
// tcxMQTTMessage.h - MQTT message struct
// =============================================================================

#include <string>
#include <vector>
#include <cstdint>

namespace tcx::mqtt {

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

} // namespace tcx::mqtt

// -----------------------------------------------------------------------------
// Backward compatibility. The canonical namespace is now `tcx::mqtt`. These
// silent aliases keep older code compiling: flat `tcx::MQTTMessage` and legacy
// `trussc::MQTTMessage`. DEPRECATED — removed in v1.0.0.
// (No [[deprecated]] attribute: under the usual `using namespace tc;` it would
//  warn on idiomatic unqualified use too. See tcxMQTT README for migration.)
// -----------------------------------------------------------------------------
namespace tcx    { using mqtt::MQTTMessage; } // deprecated: remove at v1.0.0
namespace trussc { using tcx::mqtt::MQTTMessage; } // deprecated: remove at v1.0.0
