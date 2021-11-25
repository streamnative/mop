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
package io.streamnative.pulsar.handlers.mqtt.messages.factory;

import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5SubReasonCode;


/**
 * Factory pattern, used to create mqtt protocol disconnection acknowledgement
 * message.
 *
 * @see Mqtt5DisConnReasonCode
 */
public class MqttDisConnAckMessageHelper{


    /**
     * Create Mqtt 5 disconnection acknowledgement with no property.
     *
     * @param code - MqttDisconnectReasonCode
     * @return - MqttMessage
     * @see Mqtt5DisConnReasonCode
     */
    public static MqttMessage createMqtt5(Mqtt5DisConnReasonCode code) {
        return createMqtt5(code, MqttProperties.NO_PROPERTIES);
    }

    /**
     * Create Mqtt 5 disconnection acknowledgement with property.
     *
     * @param code      - MqttDisconnectReasonCode
     * @param reasonStr - Reason string
     * @return - MqttMessage
     * @see Mqtt5SubReasonCode
     * @see MqttProperties
     */
    public static MqttMessage createMqtt5(Mqtt5DisConnReasonCode code, String reasonStr) {
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        reasonStr);
        mqttProperties.add(reasonStringProperty);
        return createMqtt5(code, mqttProperties);
    }

    /**
     * Create Mqtt 5 disconnection acknowledgement with property.
     *
     * @param code       - MqttDisconnectReasonCode
     * @param properties - MqttProperties
     * @return - MqttMessage
     * @see Mqtt5DisConnReasonCode
     * @see MqttProperties
     */
    public static MqttMessage createMqtt5(Mqtt5DisConnReasonCode code, MqttProperties properties) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.DISCONNECT, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttReasonCodeAndPropertiesVariableHeader header =
                new MqttReasonCodeAndPropertiesVariableHeader(code.byteValue(), properties);
        return MqttMessageFactory.newMessage(fixedHeader, header, null);
    }
}
