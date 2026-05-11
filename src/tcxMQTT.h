#pragma once

// =============================================================================
// tcxMQTT - TrussC MQTT addon
// MQTT 3.1.1 client wrapping lwmqtt (MIT, src/vendor/lwmqtt/) over TrussC
// TcpClient. Supports both async (Event<T> + EventListener) and sync
// (hasNewMessage / getNextMessage) usage patterns.
// =============================================================================

#include "tcxMQTTMessage.h"
#include "tcxMQTTClient.h"
