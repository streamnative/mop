package io.streamnative.pulsar.handlers.mqtt.exception;

import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.Getter;

public class MQTTQosNotSupportException extends MQTTServerException {
    @Getter
    private final MqttMessageType condition;

    public MQTTQosNotSupportException(MqttMessageType condition,
                                      String clientId, int qos) {
        super(String.format("The server do not support that qos clientId=%s qos=%s",
                clientId, qos));
        this.condition = condition;
    }
}
