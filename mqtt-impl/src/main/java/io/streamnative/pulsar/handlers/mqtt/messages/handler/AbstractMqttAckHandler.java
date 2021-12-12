package io.streamnative.pulsar.handlers.mqtt.messages.handler;

import static io.netty.handler.codec.mqtt.MqttMessageType.CONNACK;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.CONNECT_ACK;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.DISCONNECTED;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.ESTABLISHED;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerUnavailableException;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.List;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class AbstractMqttAckHandler implements ProtocolAckHandler {

    abstract MqttMessage getConnAckOkMessage(Connection connection);

    abstract MqttMessage getUnSubAckMessage(Connection connection, int packetId);

    abstract MqttMessage getPubAckMessage(Connection connection, int packetId);

    @Override
    public void connOk(Connection connection) {
        String clientId = connection.getClientId();
        boolean ret = connection.assignState(DISCONNECTED, CONNECT_ACK);
        if (!ret) {
            throw new MQTTServerUnavailableException(CONNACK, clientId);
        }
        MqttMessage ackOkMessage = getConnAckOkMessage(connection);
        connection.getChannel().writeAndFlush(ackOkMessage).addListener(future -> {
            if (future.isSuccess()) {
                if (log.isDebugEnabled()) {
                    log.debug("The CONNECT message has been processed. CId={}", clientId);
                }
                connection.assignState(CONNECT_ACK, ESTABLISHED);
                log.info("current connection state : {}", connection.getConnectionState(connection));
            }
        });
    }


    @Override
    public void subOk(Connection connection, int messageID, List<MqttQoS> qosList) {
        String clientId = connection.getClientId();
        MqttQoS[] mqttQoS = qosList.toArray(new MqttQoS[]{});
        MqttMessage subAckOkMessage = MqttMessageBuilders.subAck()
                .addGrantedQoses(mqttQoS)
                .packetId(messageID)
                .build();
        connection.getChannel().writeAndFlush(subAckOkMessage);
        if (log.isDebugEnabled()) {
            log.debug("Sending SUB-ACK message {} to {}", subAckOkMessage, clientId);
        }
    }

    @Override
    public void unSubOk(Connection connection, int messageID) {
        String clientId = connection.getClientId();
        MqttMessage unSubAckMessage = getUnSubAckMessage(connection, messageID);
        connection.getChannel().writeAndFlush(unSubAckMessage);
        if (log.isDebugEnabled()) {
            log.debug("Sending UNSUBACK message {} to {}", unSubAckMessage, clientId);
        }
    }

    @Override
    public void pubOk(Connection connection, String topic, int packetId) {
        Channel channel = connection.getChannel();
        String clientId = NettyUtils.getClientId(channel);
        MqttMessage pubAckMessage = getPubAckMessage(connection, packetId);
        channel.writeAndFlush(pubAckMessage).addListener(future -> {
            if (future.isSuccess()) {
                // decrement server receive publish message counter
                connection.decrementServerReceivePubMessage();
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Send Pub Ack {} to {}", topic, packetId,
                            clientId);
                }
            } else {
                log.warn("[{}] Failed to send Pub Ack {} to {}", topic, packetId, future.cause());
            }
        });
    }
}
