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
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.DisconnectAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.PublishAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.SubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.UnsubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnectAckHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttDisconnectAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttPubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttUnsubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract ack handler.
 */
@Slf4j
public abstract class AbstractAckHandler implements AckHandler {

    abstract MqttMessage getConnAckMessage(Connection connection);

    abstract MqttMessage getSubscribeAckMessage(Connection connection, SubscribeAck subscribeAck);

    abstract MqttMessage getUnsubscribeAckMessage(Connection connection, UnsubscribeAck unsubscribeAck);

    abstract MqttMessage getPublishAckMessage(Connection connection, PublishAck publishAck);

    abstract MqttMessage getDisconnectAckMessage(Connection connection, DisconnectAck disconnectAck);

    @Override
    public ChannelFuture sendConnAck(Connection connection) {
        String clientId = connection.getClientId();
        if (!connection.assignState(DISCONNECTED, CONNECT_ACK)) {
            log.warn("Unable to assign the state from : {} to : {} for CId={}, close channel",
                    DISCONNECTED, CONNECT_ACK, clientId);
            MqttAdapterMessage adapterMsg = new MqttAdapterMessage(connection.getClientId(),
                    MqttConnectAckHelper.errorBuilder()
                        .serverUnavailable(connection.getProtocolVersion())
                        .reasonString(String.format("Unable to assign the server state from : %s to : %s",
                            DISCONNECTED, CONNECT_ACK))
                        .build());
            adapterMsg.setAdapter(connection.isAdapter());
            return connection.sendThenClose(adapterMsg);
        }
        MqttAdapterMessage adapterMsg = new MqttAdapterMessage(connection.getClientId(), getConnAckMessage(connection));
        adapterMsg.setAdapter(connection.isAdapter());
        return connection.send(adapterMsg)
                .addListener(future -> {
                    if (future.isSuccess()) {
                        if (log.isDebugEnabled()) {
                            log.debug("The CONNECT message has been processed. CId={}", clientId);
                        }
                        connection.assignState(CONNECT_ACK, ESTABLISHED);
                        log.info("current connection state : {}", connection.getState());
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
            MqttAdapterMessage adapterMsg = new MqttAdapterMessage(connection.getClientId(), subAckMessage);
            adapterMsg.setAdapter(connection.isAdapter());
            return connection.send(adapterMsg);
        } else {
            MqttMessage subErrorAck = errorBuilder(connection.getProtocolVersion())
                    .errorReason(subscribeAck.getErrorReason())
                    .packetId(subscribeAck.getPacketId())
                    .reasonString(subscribeAck.getReasonStr())
                    .build();
            MqttAdapterMessage adapterMsg = new MqttAdapterMessage(connection.getClientId(), subErrorAck);
            adapterMsg.setAdapter(connection.isAdapter());
            return connection.sendThenClose(adapterMsg);
        }
    }

    @Override
    public ChannelFuture sendDisconnectAck(Connection connection, DisconnectAck disconnectAck) {
        if (MqttUtils.isMqtt5(connection.getProtocolVersion()) || connection.isAdapter()) {
            if (disconnectAck.isSuccess()) {
                MqttMessage discAckMessage = getDisconnectAckMessage(connection, disconnectAck);
                MqttAdapterMessage adapterMsg = new MqttAdapterMessage(connection.getClientId(), discAckMessage);
                adapterMsg.setAdapter(connection.isAdapter());
                return connection.sendThenClose(adapterMsg);
            } else {
                MqttMessage disErrorAck =
                        MqttDisconnectAckMessageHelper.errorBuilder(connection.getProtocolVersion())
                                .reasonCode(disconnectAck.getReasonCode())
                                .reasonString(disconnectAck.getReasonString())
                                .build();
                MqttAdapterMessage adapterMsg = new MqttAdapterMessage(connection.getClientId(), disErrorAck);
                adapterMsg.setAdapter(connection.isAdapter());
                return connection.sendThenClose(adapterMsg);
            }
        } else {
            return connection.getChannel().close();
        }
    }

    @Override
    public ChannelFuture sendPublishAck(Connection connection, PublishAck publishAck) {
        if (publishAck.isSuccess()){
            MqttMessage publishAckMessage = getPublishAckMessage(connection, publishAck);
            MqttAdapterMessage adapterMsg = new MqttAdapterMessage(connection.getClientId(), publishAckMessage);
            adapterMsg.setAdapter(connection.isAdapter());
            return connection.send(adapterMsg);
        } else {
            if (MqttUtils.isMqtt5(connection.getProtocolVersion()) || connection.isAdapter()) {
                MqttMessage pubErrorAck = MqttPubAckMessageHelper
                        .errorBuilder(connection.getProtocolVersion())
                        // If adapter channel, it should send back to adapter, so packetId should be 1~65535
                        .packetId(publishAck.getPacketId() == -1 ? 1 : publishAck.getPacketId())
                        .reasonCode(publishAck.getReasonCode())
                        .reasonString(publishAck.getReasonString())
                        .build();
                MqttAdapterMessage adapterMsg = new MqttAdapterMessage(connection.getClientId(), pubErrorAck);
                adapterMsg.setAdapter(connection.isAdapter());
                return connection.sendThenClose(adapterMsg);
            } else {
                // mqtt 3.x do not have any ack.
                return connection.getChannel().close();
            }
        }
    }

    @Override
    public ChannelFuture sendUnsubscribeAck(Connection connection, UnsubscribeAck unsubscribeAck) {
        if (unsubscribeAck.isSuccess()) {
            MqttMessage unsubscribeAckMessage = getUnsubscribeAckMessage(connection, unsubscribeAck);
            if (log.isDebugEnabled()) {
                log.debug("Sending UNSUBACK message {} to {}", unsubscribeAck, connection.getClientId());
            }
            MqttAdapterMessage adapterMsg = new MqttAdapterMessage(connection.getClientId(), unsubscribeAckMessage);
            adapterMsg.setAdapter(connection.isAdapter());
            return connection.send(adapterMsg);
        } else {
            if (MqttUtils.isMqtt5(connection.getProtocolVersion()) || connection.isAdapter()) {
                MqttMessage unsubscribeErrorAck = MqttUnsubAckMessageHelper
                        .errorBuilder(connection.getProtocolVersion())
                        .packetId(unsubscribeAck.getPacketId())
                        .reasonCode(unsubscribeAck.getReasonCode())
                        .reasonString(unsubscribeAck.getReasonString())
                        .build();
                MqttAdapterMessage adapterMsg = new MqttAdapterMessage(connection.getClientId(), unsubscribeErrorAck);
                adapterMsg.setAdapter(connection.isAdapter());
                return connection.sendThenClose(adapterMsg);
            } else {
                // mqtt 3.x do not have unsubscribe ack.
                return connection.getChannel().close();
            }
        }
    }
}
