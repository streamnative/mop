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

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;


public class MqttV3xAckHandler extends AbstractMqttAckHandler {
    @Override
    MqttMessage getConnAckOkMessage(Connection connection) {
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_ACCEPTED.convertToNettyKlass())
                .sessionPresent(!connection.isCleanSession())
                .build();
    }

    @Override
    MqttMessage getUnSubAckMessage(Connection connection, int messageID) {
        return MqttMessageBuilders
                .unsubAck()
                .packetId(messageID)
                .build();
    }

    @Override
    MqttMessage getPubAckMessage(Connection connection, int packetId) {
        return MqttMessageBuilders
                .pubAck()
                .packetId(packetId)
                .build();
    }

    @Override
    public void disconnectOk(Connection connection) {
        Channel channel = connection.getChannel();
        MqttMessage msg = MqttMessageBuilders
                .disconnect()
                .reasonCode(Mqtt5DisConnReasonCode.NORMAL.byteValue())
                .build();
        channel.writeAndFlush(msg);
        channel.close();
    }
}
