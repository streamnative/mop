package io.streamnative.pulsar.handlers.mqtt.exception;

public class MQTTClientIdentifierNotValidException extends MQTTServerException {
    public MQTTClientIdentifierNotValidException(String userName) {
        super(String.format("The MQTT client ID cannot be empty. Username=%s", userName));
    }
}
