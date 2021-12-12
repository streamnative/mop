package io.streamnative.pulsar.handlers.mqtt.exception;

import lombok.Getter;

public class MQTTProtocolVersionNotSupportException extends MQTTServerException {
    @Getter
    private String clientId;
    @Getter
    private int protocolVersion;

    public MQTTProtocolVersionNotSupportException(String clientId, int protocolVersion) {
        super(String.format("MQTT protocol version is not valid. clientId=%s protocolVersion=%s"
                , clientId, protocolVersion));
        this.clientId = clientId;
        this.protocolVersion = protocolVersion;
    }
};
