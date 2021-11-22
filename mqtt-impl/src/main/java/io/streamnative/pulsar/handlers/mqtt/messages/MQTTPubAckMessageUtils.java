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
package io.streamnative.pulsar.handlers.mqtt.messages;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPubReplyMessageVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttPubAckReasonCode;

/**
 * Mqtt publish acknowledgement message factory.
 */
public class MQTTPubAckMessageUtils {

    /**
     * Create Mqtt 5 publish acknowledgement with no property.
     *
     * @param packetId             - Mqtt packet id
     * @param mqttPubAckReasonCode - MqttPubAcReasonCode
     * @return - MqttMessage
     * @see MqttPubAckReasonCode
     */
    public static MqttMessage createMqtt5(int packetId, MqttPubAckReasonCode mqttPubAckReasonCode) {
        return createMqtt5(packetId, mqttPubAckReasonCode, MqttProperties.NO_PROPERTIES);
    }

    /**
     * Create error Mqtt 5 publish acknowledgement with reason string.
     *
     * @param packetId             - Mqtt packet id
     * @param mqttPubAckReasonCode - MqttPubAcReasonCode
     * @param reasonStr            - Reason string
     * @return - MqttMessage
     * @see MqttPubAckReasonCode
     */
    public static MqttMessage createMqtt5(int packetId, MqttPubAckReasonCode mqttPubAckReasonCode, String reasonStr) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        reasonStr);
        properties.add(reasonStringProperty);
        return createMqtt5(packetId, mqttPubAckReasonCode, properties);
    }

    /**
     * Create Mqtt 5 publish acknowledgement with property.
     *
     * @param packetId             - Mqtt packet id
     * @param mqttPubAckReasonCode - MqttPubAcReasonCode
     * @param properties           - MqttProperties
     * @return - MqttMessage
     * @see MqttPubAckReasonCode
     * @see MqttProperties
     */
    public static MqttMessage createMqtt5(int packetId, MqttPubAckReasonCode mqttPubAckReasonCode,
                                          MqttProperties properties) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttPubReplyMessageVariableHeader mqttPubReplyMessageVariableHeader =
                new MqttPubReplyMessageVariableHeader(packetId, mqttPubAckReasonCode.getByteValue(),
                        properties);
        return MqttMessageFactory.newMessage(fixedHeader, mqttPubReplyMessageVariableHeader, null);
    }

    /**
     * Create mqtt publish acknowledgement message that version is lower than 5.0.
     *
     * @param packetId - Mqtt packet id
     * @return - MqttMessage
     */
    public static MqttMessage createMqtt(int packetId) {
        return new MqttPubAckMessage(new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(packetId));
    }

}
