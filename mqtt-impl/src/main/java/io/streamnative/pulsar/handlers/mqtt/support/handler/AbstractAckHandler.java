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
package io.streamnative.pulsar.handlers.mqtt.support.handler;

import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.CONNECT_ACK;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.DISCONNECTED;
import static io.streamnative.pulsar.handlers.mqtt.Connection.ConnectionState.ESTABLISHED;
import static io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper.errorBuilder;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.PublishAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.DisconnectAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.SubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnectAckHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttDisconnectAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttPubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract ack handler.
 */
@Slf4j
public abstract class AbstractAckHandler implements AckHandler {

    abstract MqttMessage getConnAckMessage(Connection connection);

    abstract MqttMessage getSubscribeAckMessage(Connection connection, SubscribeAck subscribeAck);

    abstract MqttMessage getPublishAckMessage(Connection connection, PublishAck publishAck);

    abstract MqttMessage getDisconnectAckMessage(Connection connection, DisconnectAck disconnectAck);

    @Override
    public ChannelFuture sendConnAck(Connection connection) {
        String clientId = connection.getClientId();
        if (!connection.assignState(DISCONNECTED, CONNECT_ACK)) {
            log.warn("Unable to assign the state from : {} to : {} for CId={}, close channel",
                    DISCONNECTED, CONNECT_ACK, clientId);
            return connection.sendThenClose(MqttConnectAckHelper.errorBuilder()
                    .serverUnavailable(connection.getProtocolVersion())
                    .reasonString(String.format("Unable to assign the server state from : %s to : %s",
                            DISCONNECTED, CONNECT_ACK))
                    .build());
        }
        return connection.send(getConnAckMessage(connection)).addListener(future -> {
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
    public ChannelFuture sendSubscribeAck(Connection connection, SubscribeAck subscribeAck) {
        if (subscribeAck.isSuccess()){
            String clientId = connection.getClientId();
            MqttMessage subAckMessage = getSubscribeAckMessage(connection, subscribeAck);
            if (log.isDebugEnabled()) {
                log.debug("Sending SUB-ACK message {} to {}", subAckMessage, clientId);
            }
            return connection.send(subAckMessage);
        } else {
            MqttMessage subErrorAck = errorBuilder(connection.getProtocolVersion())
                    .errorReason(subscribeAck.getErrorReason())
                    .packetId(subscribeAck.getPacketId())
                    .reasonString(subscribeAck.getReasonStr())
                    .build();
            return connection.sendThenClose(subErrorAck);
        }
    }

    @Override
    public ChannelFuture sendDisconnectAck(Connection connection, DisconnectAck disconnectAck) {
        if (disconnectAck.isSuccess()) {
            MqttMessage disconnectAckMessage = getDisconnectAckMessage(connection, disconnectAck);
            return connection.sendThenClose(disconnectAckMessage);
        } else {
            MqttMessage disconnectErrorAck =
                    MqttDisconnectAckMessageHelper.errorBuilder(connection.getProtocolVersion())
                            .errorReason(disconnectAck.getErrorReason())
                            .reasonString(disconnectAck.getReasonStr())
                            .build();
            return connection.sendThenClose(disconnectErrorAck);
        }
    }

    @Override
    public ChannelFuture sendPublishAck(Connection connection, PublishAck publishAck) {
        if (publishAck.isSuccess()){
            MqttMessage publishAckMessage = getPublishAckMessage(connection, publishAck);
            return connection.send(publishAckMessage);
        } else {
            if (MqttUtils.isMqtt5(connection.getProtocolVersion())) {
                MqttMessage pubErrorAck = MqttPubAckMessageHelper
                        .errorBuilder(connection.getProtocolVersion())
                        .packetId(publishAck.getPacketId())
                        .errorReason(publishAck.getErrorReason())
                        .reasonString(publishAck.getReasonString())
                        .build();
                return connection.sendThenClose(pubErrorAck);
            } else {
                // mqtt 3.x do not have any ack.
                return connection.getChannel().close();
            }
        }
    }
}
