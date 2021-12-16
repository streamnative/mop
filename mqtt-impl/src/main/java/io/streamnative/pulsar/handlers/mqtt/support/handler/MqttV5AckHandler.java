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
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;

/**
 * MQTT5 ack handler.
 */
public class MqttV5AckHandler extends AbstractAckHandler {

    @Override
    MqttMessage getConnAckSuccessMessage(Connection connection) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.IntegerProperty property =
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(),
                        connection.getServerReceivePubMaximum());
        properties.add(property);
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt5ConnReasonCode.SUCCESS.convertToNettyKlass())
                .sessionPresent(!connection.isCleanSession())
                .properties(properties)
                .build();
    }

    @Override
    MqttMessage getConnAckServerUnAvailableMessage(Connection connection) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        String.format("Unable to assign the state from : %s to : %s."
                                , DISCONNECTED, CONNECT_ACK));
        properties.add(reasonStringProperty);
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt5ConnReasonCode.SERVER_UNAVAILABLE.convertToNettyKlass())
                .sessionPresent(false)
                .properties(properties)
                .build();
    }

    @Override
    MqttMessage getConnAckQosNotSupportedMessage(Connection connection) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        "MQTT protocol qos not supported.");
        properties.add(reasonStringProperty);
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt5ConnReasonCode.QOS_NOT_SUPPORTED.convertToNettyKlass())
                .sessionPresent(false)
                .properties(properties)
                .build();
    }

    @Override
    MqttMessage getConnAckClientIdentifierInvalidMessage(Connection connection) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        "The MQTT client ID cannot be empty. ");
        properties.add(reasonStringProperty);
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt5ConnReasonCode.CLIENT_IDENTIFIER_NOT_VALID.convertToNettyKlass())
                .sessionPresent(false)
                .properties(properties)
                .build();
    }

    @Override
    MqttMessage getConnAuthenticationFailAck(Connection connection) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        "Invalid or incorrect authentication.");
        properties.add(reasonStringProperty);
        return MqttMessageBuilders.connAck()
                .returnCode(Mqtt5ConnReasonCode.BAD_USERNAME_OR_PASSWORD.convertToNettyKlass())
                .sessionPresent(false)
                .properties(properties)
                .build();
    }
}
