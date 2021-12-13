/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt.messages.handler;

import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.CONNECT_ACK;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.DISCONNECTED;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.ESTABLISHED;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.List;
import lombok.extern.slf4j.Slf4j;


/**
 * ack method handler.
 * This handler abstract all same behavior to handle success ack in mqtt server.
 * @see MqttV3xAckHandler
 * @see MqttV5AckHandler
 */
@Slf4j
public abstract class AbstractMqttAckHandler implements ProtocolAckHandler {

    abstract MqttMessage getConnAckOkMessage(Connection connection);

    abstract MqttMessage getUnSubAckMessage(Connection connection, int packetId);

    abstract MqttMessage getPubAckMessage(Connection connection, int packetId);

    @Override
    public void connOk(Connection connection) {
        String clientId = connection.getClientId();
        Channel channel = connection.getChannel();
        boolean ret = connection.assignState(DISCONNECTED, CONNECT_ACK);
        if (!ret) {
            int protocolVersion = NettyUtils.getProtocolVersion(channel);
            log.warn("Unable to assign the state from : {} to : {} for CId={}, close channel",
                    DISCONNECTED, CONNECT_ACK, clientId);
            MqttMessage mqttConnAckMessage = MqttUtils.isMqtt5(protocolVersion)
                    ? MqttConnAckMessageHelper.createMqtt5(Mqtt5ConnReasonCode.SERVER_UNAVAILABLE,
                    String.format("Unable to assign the state from : %s to : %s for CId=%s, close channel"
                            , DISCONNECTED, CONNECT_ACK, clientId)) :
                    MqttConnAckMessageHelper.createConnAck(Mqtt3ConnReasonCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
            channel.writeAndFlush(mqttConnAckMessage);
            channel.close();
        }
        MqttMessage ackOkMessage = getConnAckOkMessage(connection);
        channel.writeAndFlush(ackOkMessage).addListener(future -> {
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
