package io.streamnative.pulsar.handlers.mqtt.exception;

import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.CONNECT_ACK;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.DISCONNECTED;
import io.netty.handler.codec.mqtt.MqttMessageType;

public class MQTTServerUnavailableException extends MQTTServerException {

    public MQTTServerUnavailableException(MqttMessageType connack, String clientId) {
        super(String.format("Unable to assign the state from : %s to : %s for CId=%s, close channel",
                DISCONNECTED, CONNECT_ACK, clientId));
    }
}
