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

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.DisconnectAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.PublishAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.SubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.UnsubscribeAck;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttConnectAckHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttDisconnectAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttPubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttUnsubAckMessageHelper;

/**
 * Mqtt3x ack handler.
 */
public class MqttV3xAckHandler extends AbstractAckHandler {

    @Override
    MqttMessage getConnAckMessage(Connection connection) {
        return MqttConnectAckHelper.builder()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_ACCEPTED.convertToNettyKlass())
                .sessionPresent(!connection.getClientRestrictions().isCleanSession())
                .build();
    }

    @Override
    MqttMessage getSubscribeAckMessage(Connection connection, SubscribeAck subscribeAck) {
        return MqttSubAckMessageHelper.builder()
                .packetId(subscribeAck.getPacketId())
                .addGrantedQoses(subscribeAck.getGrantedQoses().toArray(new MqttQoS[]{}))
                .build();
    }

    @Override
    MqttMessage getUnsubscribeAckMessage(Connection connection, UnsubscribeAck unsubscribeAck) {
        return MqttUnsubAckMessageHelper.builder()
                .packetId(unsubscribeAck.getPacketId())
                .build();
    }

    @Override
    MqttMessage getDisconnectAckMessage(Connection connection, DisconnectAck disconnectAck) {
        return MqttDisconnectAckMessageHelper
                .builder()
                .reasonCode(MqttConnectReturnCode.CONNECTION_ACCEPTED.byteValue())
                .build();
    }

    @Override
    MqttMessage getPublishAckMessage(Connection connection, PublishAck publishAck) {
        return MqttPubAckMessageHelper.builder()
                .packetId(publishAck.getPacketId())
                .reasonCode(MqttConnectReturnCode.CONNECTION_ACCEPTED.byteValue())
                .build();
    }
}
