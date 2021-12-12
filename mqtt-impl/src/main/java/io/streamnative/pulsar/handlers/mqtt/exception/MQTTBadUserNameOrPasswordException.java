package io.streamnative.pulsar.handlers.mqtt.exception;

import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.Getter;

public class MQTTBadUserNameOrPasswordException extends MQTTServerException {
    @Getter
    private final MqttMessageType condition;

    public MQTTBadUserNameOrPasswordException(MqttMessageType condition, String clientId, String username) {
        super(String.format("Invalid or incorrect authentication. CId=%s, username=%s", clientId, username));
        this.condition = condition;
    }
}
