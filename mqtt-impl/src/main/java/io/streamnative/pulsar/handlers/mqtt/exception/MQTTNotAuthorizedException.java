package io.streamnative.pulsar.handlers.mqtt.exception;

import io.netty.handler.codec.mqtt.MqttMessageType;
import lombok.Getter;

public class MQTTNotAuthorizedException extends MQTTServerException {
    @Getter
    private final MqttMessageType condition;
    @Getter
    private final String clientId;
    @Getter
    private final int packetId;

    public MQTTNotAuthorizedException(MqttMessageType condition, int packetId,
                                      String topicName, String userRole, String clientId) {
        super(String.format("No authorization to topic=%s, userRole=%s, CId= %s",
                topicName, userRole, clientId));
        this.condition = condition;
        this.clientId = clientId;
        this.packetId = packetId;
    }
    public MQTTNotAuthorizedException(MqttMessageType condition, int packetId,
                                      String userRole, String clientId) {
        super(String.format("No authorization userRole=%s, CId= %s",
                 userRole, clientId));
        this.condition = condition;
        this.clientId = clientId;
        this.packetId = packetId;
    }
}
