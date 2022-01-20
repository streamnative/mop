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
import io.streamnative.pulsar.handlers.mqtt.messages.ack.PublishAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.SubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.UnsubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5UnsubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnectAckHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttDisconnectAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttPubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttUnsubAckMessageHelper;

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
    MqttMessage getUnsubscribeAckMessage(Connection connection, UnsubscribeAck unsubscribeAck) {
        return MqttUnsubAckMessageHelper.builder()
                .packetId(unsubscribeAck.getPacketId())
                // Because of MQTT protocol version 5 has non-error reason code - NO_SUBSCRIPTION_EXISTED
                .addReasonCode(unsubscribeAck.getReasonCode() != null
                        ? unsubscribeAck.getReasonCode().shortValue() : Mqtt5UnsubReasonCode.SUCCESS.shortValue())
                .build();
    }

    @Override
    MqttMessage getDisconnectAckMessage(Connection connection, DisconnectAck disconnectAck) {
        // Now this section same to V3, but Mqtt V5 has another different feature that will be supported in the future.
        return MqttDisconnectAckMessageHelper.builder()
                .reasonCode(Mqtt5DisConnReasonCode.NORMAL.byteValue())
                .build();
    }

    @Override
    MqttMessage getPublishAckMessage(Connection connection, PublishAck publishAck) {
        return MqttPubAckMessageHelper.builder()
                .packetId(publishAck.getPacketId())
                // Because of MQTT protocol version 5 has non-error reason code - NoMatchingSubscription
                .reasonCode(publishAck.getReasonCode() != null
                        ? publishAck.getReasonCode().byteValue() : Mqtt5PubReasonCode.SUCCESS.byteValue())
                .build();
    }
}
