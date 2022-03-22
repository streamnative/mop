package io.streamnative.pulsar.handlers.mqtt.messages.ack;

import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnectAckHelper;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ConnectAck {
    private final boolean success;
    private final MqttConnectAckHelper.ErrorReason errorReason;
    private final String reasonStr;
}
