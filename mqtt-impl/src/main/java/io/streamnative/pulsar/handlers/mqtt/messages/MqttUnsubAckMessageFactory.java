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
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubAckPayload;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttUnsubAckReasonCode;

/**
 * Mqtt unsubscribe acknowledgement message factory.
 */
public class MqttUnsubAckMessageFactory {

    /**
     * Create Mqtt 5 unsubscribe acknowledgement with no property.
     *
     * @param messageID              - Mqtt message id.
     * @param unsubscribeReasonCodes - MqttUnsubAckReasonCode
     * @return - MqttMessage
     * @see MqttUnsubAckReasonCode
     */
    public static MqttMessage createMqtt5(int messageID, MqttUnsubAckReasonCode unsubscribeReasonCodes) {
        return createMqtt5(messageID, unsubscribeReasonCodes, MqttProperties.NO_PROPERTIES);
    }

    /**
     * Create error Mqtt 5 unsubscribe acknowledgement with reason string.
     *
     * @param messageID              - Mqtt message id
     * @param unsubscribeReasonCodes - MqttUnsubAckReasonCode
     * @param reasonStr              - Reason string
     * @return - MqttMessage
     * @see MqttUnsubAckReasonCode
     */
    public static MqttMessage createMqtt5(int messageID, MqttUnsubAckReasonCode unsubscribeReasonCodes,
                                          String reasonStr) {
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        reasonStr);
        mqttProperties.add(reasonStringProperty);
        return createMqtt5(messageID, unsubscribeReasonCodes, mqttProperties);
    }

    /**
     * Create Mqtt 5 unsubscribe acknowledgement with property.
     *
     * @param messageID              - Mqtt message id
     * @param unsubscribeReasonCodes - MqttUnsubAckReasonCode
     * @param properties             - MqttProperties
     * @return - MqttMessage
     * @see MqttUnsubAckReasonCode
     * @see MqttProperties
     */
    public static MqttMessage createMqtt5(int messageID, MqttUnsubAckReasonCode unsubscribeReasonCodes,
                                          MqttProperties properties) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttMessageIdAndPropertiesVariableHeader mqttMessageIdAndPropertiesVariableHeader =
                new MqttMessageIdAndPropertiesVariableHeader(messageID, properties);
        MqttUnsubAckPayload mqttUnsubAckPayload =
                new MqttUnsubAckPayload(unsubscribeReasonCodes.value());
        return MqttMessageFactory.newMessage(fixedHeader, mqttMessageIdAndPropertiesVariableHeader,
                mqttUnsubAckPayload);
    }

    /**
     * Create mqtt unsubscribe acknowledgement message that version is lower than 5.0.
     *
     * @param messageID - Mqtt message id
     * @return - MqttMessage
     */
    public static MqttMessage createMqtt(int messageID) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        return new MqttUnsubAckMessage(fixedHeader, MqttMessageIdVariableHeader.from(messageID));
    }


}
