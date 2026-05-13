#include <TrussC.h>
#include "tcApp.h"

int main() {
    WindowSettings settings;
    settings.title  = "tcxMQTT - tlsPubSub (MQTTS via tcxTls)";
    settings.width  = 640;
    settings.height = 400;
    settings.highDpi = false;
    return TC_RUN_APP(tcApp, settings);
}
