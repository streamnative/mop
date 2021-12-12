package io.streamnative.pulsar.handlers.mqtt.exception;

import lombok.Getter;

public class MQTTDisconnectProtocolErrorException extends MQTTServerException {
    @Getter
    private final String clientId;

    public MQTTDisconnectProtocolErrorException(String clientId, Integer sessionExpireInterval) {
        super(String.format("The client %s disconnect with wrong session expire interval value. the value is %s",
                clientId, sessionExpireInterval));
        this.clientId = clientId;
    }
}
