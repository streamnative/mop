package io.streamnative.pulsar.handlers.mqtt.messages.handler;


import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import java.util.List;

public interface ProtocolAckHandler {

    void connOk(Connection connection);

    void subOk(Connection connection, int messageID, List<MqttQoS> qosList);

    void unSubOk(Connection connection, int messageID);

    void pubOk(Connection connection, String topic, int packetId);

    void disconnectOk(Connection connection);
}
