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
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract ack handler.
 */
@Slf4j
public abstract class AbstractAckHandler implements AckHandler {

    abstract MqttMessage getConnAckMessage(Connection connection);

    abstract MqttMessage getServerUnAvailableMessage(Connection connection);

    @Override
    public void sendConnAck(Connection connection) {
        String clientId = connection.getClientId();
        if (!connection.assignState(DISCONNECTED, CONNECT_ACK)) {
            log.warn("Unable to assign the state from : {} to : {} for CId={}, close channel",
                    DISCONNECTED, CONNECT_ACK, clientId);
            MqttMessage connAckServerUnAvailableMessage = getServerUnAvailableMessage(connection);
            connection.sendThenClose(connAckServerUnAvailableMessage);
            return;
        }
        MqttMessage ackSuccessMessage = getConnAckMessage(connection);
        connection.send(ackSuccessMessage).addListener(future -> {
            if (future.isSuccess()) {
                if (log.isDebugEnabled()) {
                    log.debug("The CONNECT message has been processed. CId={}", clientId);
                }
                connection.assignState(CONNECT_ACK, ESTABLISHED);
                log.info("current connection state : {}", connection.getConnectionState(connection));
            }
        });
    }

    abstract MqttMessage getQosNotSupportedMessage(Channel channel);

    @Override
    public void sendConnNotSupported(Channel channel) {
        log.error("MQTT protocol qos not supported. the remote address is {}", channel.remoteAddress());
        MqttMessage connAckQosNotSupportedMessage = getQosNotSupportedMessage(channel);
        channel.writeAndFlush(connAckQosNotSupportedMessage);
        channel.close();
    }

    abstract MqttMessage getClientIdentifierInvalidMessage(Channel channel);

    @Override
    public void sendConnClientIdentifierInvalid(Channel channel) {
        log.error("The MQTT client ID cannot be empty. the remote address is {}",
                channel.remoteAddress());
        MqttMessage connAckClientIdentifierInvalidMessage =
                getClientIdentifierInvalidMessage(channel);
        channel.writeAndFlush(connAckClientIdentifierInvalidMessage);
        channel.close();
    }

    abstract MqttMessage getAuthenticationFailMessage(Channel channel);

    @Override
    public void sendConnAuthenticationFail(Channel channel) {
        log.error("Invalid or incorrect authentication. the remote address is {}", channel.remoteAddress());
        MqttMessage connAuthenticationFailMessage = getAuthenticationFailMessage(channel);
        channel.writeAndFlush(connAuthenticationFailMessage);
        channel.close();
    }
}
