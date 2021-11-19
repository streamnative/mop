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


import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import com.google.common.collect.Lists;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttSubAckReasonCode;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Mqtt subscribe acknowledgement message factory.
 */
public class MQTTSubAckMessageUtils {
    /**
     * Create Mqtt 5 subscribe acknowledgement with no property.
     *
     * @param messageID          - Mqtt message id.
     * @param topicSubscriptions - The list of mqttTopicSubscription.
     * @return - MqttMessage
     * @see MqttTopicSubscription
     */
    public static MqttMessage createMqtt5(int messageID, List<MqttTopicSubscription> topicSubscriptions) {
        List<MqttSubAckReasonCode> mqttSubAckReasonCodes = topicSubscriptions.stream()
                .map(sub -> MqttSubAckReasonCode.qosGranted(sub.qualityOfService()))
                .collect(Collectors.toList());
        return createMqtt5(messageID, mqttSubAckReasonCodes, MqttProperties.NO_PROPERTIES);
    }

    /**
     * Create error Mqtt 5 subscribe acknowledgement with reason string.
     *
     * @param messageID        - Mqtt message id
     * @param subAckReasonCode - MqttSubAckReasonCode
     * @param reasonStr        - Reason string
     * @return - MqttMessage
     * @see MqttSubAckReasonCode
     */
    public static MqttMessage createMqtt5(int messageID, MqttSubAckReasonCode subAckReasonCode,
                                          String reasonStr) {
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        reasonStr);
        mqttProperties.add(reasonStringProperty);
        return createMqtt5(messageID, Lists.newArrayList(subAckReasonCode), mqttProperties);
    }

    /**
     * Create Mqtt 5 subscribe acknowledgement with property.
     *
     * @param messageID         - Mqtt message id
     * @param subAckReasonCodes - MqttSubAckReasonCode
     * @param properties        - MqttProperties
     * @return - MqttMessage
     * @see MqttSubAckReasonCode
     * @see MqttProperties
     */
    public static MqttMessage createMqtt5(int messageID, List<MqttSubAckReasonCode> subAckReasonCodes,
                                          MqttProperties properties) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttMessageIdAndPropertiesVariableHeader mqttMessageIdAndPropertiesVariableHeader =
                new MqttMessageIdAndPropertiesVariableHeader(messageID, properties);
        MqttSubAckPayload mqttSubAckPayload =
                new MqttSubAckPayload(
                        subAckReasonCodes.stream().map(MqttSubAckReasonCode::value).collect(Collectors.toList()));
        return MqttMessageFactory.newMessage(fixedHeader, mqttMessageIdAndPropertiesVariableHeader,
                mqttSubAckPayload);
    }

    /**
     * Create mqtt subscribe acknowledgement message that version is lower than 5.0.
     *
     * @param messageId          - Mqtt message id.
     * @param topicSubscriptions - The list of mqttTopicSubscription.
     * @return - MqttMessage
     * @see MqttTopicSubscription
     */
    public static MqttSubAckMessage createMqtt(int messageId, List<MqttTopicSubscription> topicSubscriptions) {
        List<Integer> grantedQoSLevels = new ArrayList<>();
        for (MqttTopicSubscription req : topicSubscriptions) {
            grantedQoSLevels.add(req.qualityOfService().value());
        }
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, AT_MOST_ONCE,
                false, 0);
        MqttSubAckPayload payload = new MqttSubAckPayload(grantedQoSLevels);
        return new MqttSubAckMessage(fixedHeader, MqttMessageIdVariableHeader.from(messageId), payload);
    }
}
