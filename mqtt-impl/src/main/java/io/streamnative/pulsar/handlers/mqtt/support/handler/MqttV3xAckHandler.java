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
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;

/**
 * Mqtt3x ack handler.
 */
public class MqttV3xAckHandler extends AbstractAckHandler {

    @Override
    MqttMessage getConnAckSuccessMessage(Connection connection) {
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_ACCEPTED.convertToNettyKlass())
                .sessionPresent(!connection.isCleanSession())
                .build();
    }

    @Override
    MqttMessage getConnAckServerUnAvailableMessage(Connection connection) {
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE.convertToNettyKlass())
                .sessionPresent(false)
                .build();
    }

    @Override
    MqttMessage getConnAckQosNotSupportedMessage(Connection connection) {
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE.convertToNettyKlass())
                .sessionPresent(false)
                .build();
    }

    @Override
    MqttMessage getConnAckClientIdentifierInvalidMessage(Connection connection) {
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED.convertToNettyKlass())
                .sessionPresent(false)
                .build();
    }

    @Override
    MqttMessage getConnAuthenticationFailAck(Connection connection) {
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD.convertToNettyKlass())
                .sessionPresent(false)
                .build();
    }
}
