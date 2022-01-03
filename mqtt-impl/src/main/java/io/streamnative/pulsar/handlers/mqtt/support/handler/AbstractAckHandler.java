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
import io.netty.channel.ChannelFuture;
import static io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper.ErrorReason;
import static io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper.errorBuilder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnectAckHelper;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract ack handler.
 */
@Slf4j
public abstract class AbstractAckHandler implements AckHandler {

    abstract MqttMessage getConnAckMessage(Connection connection);

    abstract MqttMessage getSubAckMessage(Connection connection, int packetId, List<MqttQoS> grantedQoses);

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
    public void sendSubError(Connection connection, int packetId,
                             ErrorReason authorizationFail) {
        MqttMessage subErrorAck = errorBuilder(connection.getProtocolVersion())
                .errorReason(authorizationFail)
                .packetId(packetId)
                .build();
        connection.sendThenClose(subErrorAck);
    }

    @Override
    public void sendSubError(Connection connection, int packetId, String reasonStr) {
        MqttMessage subErrorAck = errorBuilder(connection.getProtocolVersion())
                .errorReason(ErrorReason.UNSPECIFIED_ERROR)
                .packetId(packetId)
                .reasonString(reasonStr)
                .build();
        connection.sendThenClose(subErrorAck);
    }

    @Override
    public void sendSubAck(Connection connection, int packetId, List<MqttQoS> grantedQoses) {
        String clientId = connection.getClientId();
        MqttMessage subAckMessage = getSubAckMessage(connection, packetId, grantedQoses);
        if (log.isDebugEnabled()) {
            log.debug("Sending SUB-ACK message {} to {}", subAckMessage, clientId);
        }
        connection.send(subAckMessage);
    }
}
