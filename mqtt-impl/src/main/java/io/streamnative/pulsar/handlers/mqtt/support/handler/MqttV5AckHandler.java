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

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.DisconnectAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.SubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnectAckHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttDisconnectAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;

/**
 * MQTT5 ack handler.
 */
public class MqttV5AckHandler extends AbstractAckHandler {

    @Override
    MqttMessage getConnAckMessage(Connection connection) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.IntegerProperty property =
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(),
                        connection.getServerReceivePubMaximum());
        properties.add(property);
        return MqttConnectAckHelper.builder()
                .returnCode(Mqtt5ConnReasonCode.SUCCESS.convertToNettyKlass())
                .sessionPresent(!connection.isCleanSession())
                .properties(properties)
                .build();
    }

    @Override
    MqttMessage getSubscribeAckMessage(Connection connection, SubscribeAck subscribeAck) {
        // Now this section same to V3, but Mqtt V5 has another different feature that will be supported in the future.
        return MqttSubAckMessageHelper.builder()
                .packetId(subscribeAck.getPacketId())
                .addGrantedQoses(subscribeAck.getGrantedQoses().toArray(new MqttQoS[]{}))
                .build();
    }

    @Override
    MqttMessage getDisconnectAckMessage(Connection connection, DisconnectAck disconnectAck) {
        // Now this section same to V3, but Mqtt V5 has another different feature that will be supported in the future.
        return MqttDisconnectAckMessageHelper.builder()
                .reasonCode(Mqtt5DisConnReasonCode.NORMAL.byteValue())
                .build();
    }
}
