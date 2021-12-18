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
import io.netty.handler.codec.mqtt.MqttMessage;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract ack handler.
 */
@Slf4j
public abstract class AbstractAckHandler implements AckHandler {

    abstract MqttMessage getConnAckMessage(Connection connection);

    @Override
    public void sendConnAck(Connection connection) {
        String clientId = connection.getClientId();
        boolean ret = connection.assignState(DISCONNECTED, CONNECT_ACK);
        if (!ret) {
            int protocolVersion = connection.getProtocolVersion();
            log.warn("Unable to assign the state from : {} to : {} for CId={}, close channel",
                    DISCONNECTED, CONNECT_ACK, clientId);
            MqttMessage mqttConnAckMessage = MqttUtils.isMqtt5(protocolVersion)
                    ? MqttConnAckMessageHelper.createMqtt5(Mqtt5ConnReasonCode.SERVER_UNAVAILABLE,
                    String.format("Unable to assign the state from : %s to : %s for CId=%s, close channel"
                            , DISCONNECTED, CONNECT_ACK, clientId)) :
                    MqttConnAckMessageHelper.createConnAck(Mqtt3ConnReasonCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
            connection.sendThenClose(mqttConnAckMessage);
            return;
        }
        MqttMessage ackOkMessage = getConnAckMessage(connection);
        connection.send(ackOkMessage).addListener(future -> {
            if (future.isSuccess()) {
                if (log.isDebugEnabled()) {
                    log.debug("The CONNECT message has been processed. CId={}", clientId);
                }
                connection.assignState(CONNECT_ACK, ESTABLISHED);
                log.info("current connection state : {}", connection.getConnectionState(connection));
            }
        });
    }
}
