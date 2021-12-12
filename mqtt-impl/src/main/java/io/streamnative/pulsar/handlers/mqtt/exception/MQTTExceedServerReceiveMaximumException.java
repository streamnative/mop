package io.streamnative.pulsar.handlers.mqtt.exception;

import lombok.Getter;

public class MQTTExceedServerReceiveMaximumException extends MQTTServerException {
    @Getter
    private final int packetId;

    public MQTTExceedServerReceiveMaximumException(int packetId, int serverReceivePubMaximum) {
        super(String.format("Client publish exceed server receive maximum , the receive maximum is %s",
                serverReceivePubMaximum));
        this.packetId = packetId;
    }
}
