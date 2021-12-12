package io.streamnative.pulsar.handlers.mqtt.messages.handler;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;


public class MqttV3xAckHandler extends AbstractMqttAckHandler {
    @Override
    MqttMessage getConnAckOkMessage(Connection connection) {
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_ACCEPTED.convertToNettyKlass())
                .sessionPresent(!connection.isCleanSession())
                .build();
    }

    @Override
    MqttMessage getUnSubAckMessage(Connection connection, int messageID) {
        return MqttMessageBuilders
                .unsubAck()
                .packetId(messageID)
                .build();
    }

    @Override
    MqttMessage getPubAckMessage(Connection connection, int packetId) {
        return MqttMessageBuilders
                .pubAck()
                .packetId(packetId)
                .build();
    }

    @Override
    public void disconnectOk(Connection connection) {
        Channel channel = connection.getChannel();
        MqttMessage msg = MqttMessageBuilders
                .disconnect()
                .reasonCode(Mqtt5DisConnReasonCode.NORMAL.byteValue())
                .build();
        channel.writeAndFlush(msg);
        channel.close();
    }
}
