package io.streamnative.pulsar.handlers.mqtt.messages.handler;


import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5UnsubReasonCode;


public class MqttV5AckHandler extends AbstractMqttAckHandler {
    @Override
    MqttMessage getConnAckOkMessage(Connection connection) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.IntegerProperty property =
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(),
                        connection.getServerReceivePubMaximum());
        properties.add(property);
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt5ConnReasonCode.SUCCESS.convertToNettyKlass())
                .sessionPresent(!connection.isCleanSession())
                .properties(properties)
                .build();
    }

    @Override
    MqttMessage getUnSubAckMessage(Connection connection, int messageID) {
        return MqttMessageBuilders
                .unsubAck()
                .addReasonCode(Mqtt5UnsubReasonCode.SUCCESS.shortValue())
                .packetId(messageID)
                .build();
    }

    @Override
    MqttMessage getPubAckMessage(Connection connection, int packetId) {
        return MqttMessageBuilders
                .pubAck()
                .reasonCode(Mqtt5PubReasonCode.SUCCESS.byteValue())
                .packetId(packetId)
                .build();
    }

    @Override
    public void disconnectOk(Connection connection) {

        connection.getChannel().close();
    }
}
