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
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;

/**
 * Factory pattern, used to create mqtt protocol connection acknowledgement
 * message.
 *
 * @see Mqtt5ConnReasonCode
 */
public class MqttConnAckMessageHelper {

    public static MqttMessage createIdentifierInvalidAck(int protocolVersion) {
        return MqttUtils.isMqtt5(protocolVersion)
                ? createConnAck(Mqtt5ConnReasonCode.CLIENT_IDENTIFIER_NOT_VALID)
                : createConnAck(Mqtt3ConnReasonCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
    }

    public static MqttMessage createAuthFailedAck(int protocolVersion) {
        return MqttUtils.isMqtt5(protocolVersion)
                ? createConnAck(Mqtt5ConnReasonCode.BAD_USERNAME_OR_PASSWORD)
                : createConnAck(Mqtt3ConnReasonCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
    }

    public static MqttMessage createUnsupportedVersionAck() {
        return createConnAck(Mqtt5ConnReasonCode.UNSUPPORTED_PROTOCOL_VERSION);
    }

    /**
     * Create Mqtt 5 connection acknowledgement with no property.
     *
     * @param conAckReasonCode - Mqtt5ConnReasonCode
     * @return - MqttMessage
     * @see Mqtt5ConnReasonCode
     */
    public static MqttMessage createConnAck(Mqtt5ConnReasonCode conAckReasonCode) {
        return createMqtt5(conAckReasonCode, false, MqttProperties.NO_PROPERTIES);
    }

    /**
     * Create Mqtt 5 connection acknowledgement with no property.
     *
     * @param conAckReasonCode - Mqtt5ConnReasonCode
     * @return - MqttMessage
     * @see Mqtt5ConnReasonCode
     */
    public static MqttMessage createConnAck(Mqtt5ConnReasonCode conAckReasonCode, boolean sessionPresent) {
        return createMqtt5(conAckReasonCode, sessionPresent, MqttProperties.NO_PROPERTIES);
    }

    /**
     * Create Mqtt 5 connection acknowledgement with receiveMaximum property.
     *
     * @param conAckReasonCode     - Mqtt5ConnReasonCode
     * @param sessionPresent       - session present
     * @param serverReceiveMaximum - server receive message maximum number
     * @return - MqttMessage
     * @see Mqtt5ConnReasonCode
     */
    public static MqttMessage createConnAck(Mqtt5ConnReasonCode conAckReasonCode, boolean sessionPresent,
                                         Integer serverReceiveMaximum) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.IntegerProperty property =
                new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(),
                        serverReceiveMaximum);
        properties.add(property);
        return createMqtt5(conAckReasonCode, sessionPresent, properties);
    }


    /**
     * Create Mqtt connection acknowledgement.
     *
     * @param connectReturnCode - Mqtt3ConnReasonCode
     * @return - MqttMessage
     * @see Mqtt3ConnReasonCode
     */
    public static MqttMessage createConnAck(Mqtt3ConnReasonCode connectReturnCode) {
      return createConnAck(connectReturnCode, false);
    }

    /**
     * Create Mqtt connection acknowledgement.
     *
     * @param connectReturnCode - Mqtt3ConnReasonCode
     * @return - MqttMessage
     * @see Mqtt3ConnReasonCode
     */
    public static MqttMessage createConnAck(Mqtt3ConnReasonCode connectReturnCode, boolean sessionPresent) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader =
                new MqttConnAckVariableHeader(connectReturnCode.convertToNettyKlass(), sessionPresent);
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
     * @param conAckReasonCode - Mqtt5ConnReasonCode
     * @param sessionPresent   - Session present
     * @param properties       - Mqtt properties
     * @return - MqttMessage
     * @see Mqtt5ConnReasonCode
     */
    private static MqttMessage createMqtt5(Mqtt5ConnReasonCode conAckReasonCode,
                                           Boolean sessionPresent, MqttProperties properties) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE,
                false, 0);
        MqttConnAckVariableHeader mqttConnAckVariableHeader =
                new MqttConnAckVariableHeader(conAckReasonCode.convertToNettyKlass(), sessionPresent, properties);
        return new MqttConnAckMessage(fixedHeader, mqttConnAckVariableHeader);
    }
}
