package io.streamnative.pulsar.handlers.mqtt;

import io.netty.util.HashedWheelTimer;
import net.jcip.annotations.ThreadSafe;

/**
 * The MQTT protocol timer container contains all of the
 * timers to help manage and observe them.
 */
@ThreadSafe
public class MqttTimer {
    private final static HashedWheelTimer COMMON_TIMER = new HashedWheelTimer();

    public static HashedWheelTimer getCommonTimer() {
        return COMMON_TIMER;
    }
}
