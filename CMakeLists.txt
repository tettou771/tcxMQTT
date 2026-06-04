cmake_minimum_required(VERSION 3.20)
message(STATUS "[tcxMQTT] Loading addon...")

# Vendored MQTT 3.1.1 protocol library (MIT). The reference POSIX socket
# backend (lwmqtt/src/posix.c) is intentionally not vendored — TrussC's
# TcpClient is used instead, so the addon stays platform-agnostic.
set(LWMQTT_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/vendor/lwmqtt/client.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/vendor/lwmqtt/packet.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/vendor/lwmqtt/helpers.c"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/vendor/lwmqtt/string.c"
)

set(TCXMQTT_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/tcxMQTTClient.cpp"
)

# Header files (for IDE visibility)
file(GLOB TCXMQTT_HEADERS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.h"
)

add_library(tcxMQTT STATIC
    ${TCXMQTT_SOURCES}
    ${LWMQTT_SOURCES}
    ${TCXMQTT_HEADERS}
)

target_include_directories(tcxMQTT
    PUBLIC  "${CMAKE_CURRENT_SOURCE_DIR}/src"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src/vendor/lwmqtt"
)

target_link_libraries(tcxMQTT PUBLIC TrussC)

# lwmqtt is C99; silence common warnings (vendor code we don't own).
# These are GCC/Clang flag spellings — MSVC's cl.exe rejects them (D8021), so
# only apply them on non-MSVC toolchains.
if(NOT MSVC)
    set_source_files_properties(${LWMQTT_SOURCES} PROPERTIES
        COMPILE_FLAGS "-Wno-unused-parameter -Wno-sign-compare"
    )
endif()

message(STATUS "[tcxMQTT] Addon loaded")
