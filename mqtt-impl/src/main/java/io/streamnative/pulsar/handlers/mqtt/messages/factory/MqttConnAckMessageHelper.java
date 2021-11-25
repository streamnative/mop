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

import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;

/**
 * Factory pattern, used to create mqtt protocol connection acknowledgement
 * message.
 *
 * @see Mqtt5ConnReasonCode
 */
public class MqttConnAckMessageHelper {

    /**
     * Create Mqtt 5 connection acknowledgement with no property.
     *
     * @param conAckReasonCode - MqttConnectReturnCode
     * @return - MqttMessage
     * @see MqttConnectReturnCode
     */
    public static MqttMessage createMqtt(Mqtt5ConnReasonCode conAckReasonCode) {
        return createMqtt5(conAckReasonCode, false, MqttProperties.NO_PROPERTIES);
    }

    /**
     * Create Mqtt connection acknowledgement.
     *
     * @param connectReturnCode - MqttConnectReturnCode
     * @return - MqttMessage
     * @see MqttConnectReturnCode
     */
    public static MqttMessage createMqtt(MqttConnectReturnCode connectReturnCode) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader =
                new MqttConnAckVariableHeader(connectReturnCode, false);
        return new MqttConnAckMessage(fixedHeader, mqttConnAckVariableHeader);
    }

    public static MqttMessage createMqtt5(Mqtt5ConnReasonCode conAckReasonCode, String reasonStr) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        reasonStr);
        properties.add(reasonStringProperty);
        return createMqtt5(conAckReasonCode, false, properties);
    }

    /**
     * Create Mqtt 5 connection acknowledgement with no property.
     *
     * @param conAckReasonCode - MqttConnectReturnCode
     * @param sessionPresent   - Session present
     * @param properties       - Mqtt properties
     * @return - MqttMessage
     * @see MqttConnectReturnCode
     */
    public static MqttMessage createMqtt5(Mqtt5ConnReasonCode conAckReasonCode,
                                          Boolean sessionPresent, MqttProperties properties) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader =
                new MqttConnAckVariableHeader(conAckReasonCode.convertToNettyKlass(), sessionPresent, properties);
        return new MqttConnAckMessage(fixedHeader, mqttConnAckVariableHeader);
    }
}
