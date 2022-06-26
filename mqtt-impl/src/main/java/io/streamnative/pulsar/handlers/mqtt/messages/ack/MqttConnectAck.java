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
package io.streamnative.pulsar.handlers.mqtt.messages.ack;


import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Enhance mqtt connect ack message builder.
 *
 * Use this class to generate message that Compatible with mqtt version 3.x and 5.x
 * This class base on #{MqttMessageBuilders}
 * @see MqttMessageBuilders
 */
public class MqttConnectAck {

    public static MqttConnectSuccessAckBuilder successBuilder(int protocolVersion) {
        return new MqttConnectSuccessAckBuilder(protocolVersion);
    }

    public static MqttConnectErrorAckBuilder errorBuilder() {
        return new MqttConnectErrorAckBuilder();
    }

    public final static class MqttConnectSuccessAckBuilder {
        private final int protocolVersion;
        private boolean cleanSession;
        private int receiveMaximum;

        private int topicAliasMaximum;

        private int maximumQos;

        private String responseInformation;

        private int maximumPacketSize;

        public MqttConnectSuccessAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttConnectSuccessAckBuilder cleanSession(boolean cleanSession) {
            this.cleanSession = cleanSession;
            return this;
        }

        public MqttConnectSuccessAckBuilder topicAliasMaximum(int topicAliasMaximum) {
            this.topicAliasMaximum = topicAliasMaximum;
            return this;
        }


        public MqttConnectSuccessAckBuilder receiveMaximum(int receiveMaximum) {
            this.receiveMaximum = receiveMaximum;
            return this;
        }

        public MqttConnectSuccessAckBuilder maximumQos(int maximumQos) {
            this.maximumQos = maximumQos;
            return this;
        }

        public MqttConnectSuccessAckBuilder responseInformation(String responseInformation) {
            this.responseInformation = responseInformation;
            return this;
        }

        public MqttConnectSuccessAckBuilder maximumPacketSize(int maximumPacketSize) {
            this.maximumPacketSize = maximumPacketSize;
            return this;
        }

        public MqttAck build() {
            MqttMessageBuilders.ConnAckBuilder commonBuilder = MqttMessageBuilders.connAck()
                    .sessionPresent(!cleanSession);
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return MqttAck.createSupportedAck(commonBuilder
                        .returnCode(Mqtt3ConnReasonCode.CONNECTION_ACCEPTED.toConnectionReasonCode())
                        .build());
            }
            MqttProperties properties = new MqttProperties();
            MqttProperties.IntegerProperty receiveMaximumProperty =
                    new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.RECEIVE_MAXIMUM.value(),
                            receiveMaximum);
            MqttProperties.IntegerProperty maximumQosProperty =
                    new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.MAXIMUM_QOS.value(),
                            maximumQos);
            // Set Subscription Identifiers Available = 0 now.
            MqttProperties.IntegerProperty subscriptionIdentifiersAvailableProperty =
                    new MqttProperties.IntegerProperty(
                            MqttProperties.MqttPropertyType.SUBSCRIPTION_IDENTIFIER_AVAILABLE.value(), 0);
            MqttProperties.IntegerProperty maximumPacketSizeProperty =
                    new MqttProperties.IntegerProperty(
                            MqttProperties.MqttPropertyType.MAXIMUM_PACKET_SIZE.value(), maximumPacketSize);
            MqttProperties.IntegerProperty topicAliasProperty =
                    new MqttProperties.IntegerProperty(MqttProperties.MqttPropertyType.TOPIC_ALIAS_MAXIMUM.value(),
                            topicAliasMaximum);
            properties.add(receiveMaximumProperty);
            properties.add(maximumQosProperty);
            properties.add(subscriptionIdentifiersAvailableProperty);
            properties.add(maximumPacketSizeProperty);
            properties.add(topicAliasProperty);
            if (StringUtils.isNotEmpty(responseInformation)) {
                MqttProperties.StringProperty responseInformationProperty =
                        new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.RESPONSE_INFORMATION.value(),
                                responseInformation);
                properties.add(responseInformationProperty);
            }
            return MqttAck.createSupportedAck(
                    commonBuilder.returnCode(Mqtt5ConnReasonCode.SUCCESS.toConnectionReasonCode())
                    .properties(properties)
                    .build());
        }

    }

    public final static class MqttConnectErrorAckBuilder {
        private int protocolVersion;
        private ErrorReason errorReason;
        private String reasonString;

        public MqttConnectErrorAckBuilder serverUnavailable(int protocolVersion) {
            this.protocolVersion = protocolVersion;
            this.errorReason = ErrorReason.SERVER_UNAVAILABLE;
            return this;
        }

        public MqttConnectErrorAckBuilder reasonString(String reasonStr) {
            this.reasonString = reasonStr;
            return this;
        }

        public MqttMessage identifierInvalid(int protocolVersion) {
            this.protocolVersion = protocolVersion;
            this.errorReason = ErrorReason.IDENTIFIER_INVALID;
            return build().getMqttMessage();
        }

        public MqttMessage authFail(int protocolVersion) {
            this.protocolVersion = protocolVersion;
            this.errorReason = ErrorReason.AUTH_FAILED;
            return build().getMqttMessage();
        }

        public MqttMessage qosNotSupport(int protocolVersion) {
            this.protocolVersion = protocolVersion;
            this.errorReason = ErrorReason.QOS_NOT_SUPPORT;
            return build().getMqttMessage();
        }

        public MqttMessage willQosNotSupport(int protocolVersion) {
            this.protocolVersion = protocolVersion;
            this.errorReason = ErrorReason.WILL_QOS_NOT_SUPPORT;
            return build().getMqttMessage();
        }

        public MqttMessage unsupportedVersion() {
            this.errorReason = ErrorReason.UNSUPPORTED_VERSION;
            return build().getMqttMessage();
        }

        public MqttMessage protocolError(int protocolVersion) {
            this.protocolVersion = protocolVersion;
            this.errorReason = ErrorReason.PROTOCOL_ERROR;
            return build().getMqttMessage();
        }

        public MqttAck build() {
            MqttMessageBuilders.ConnAckBuilder connAckBuilder = MqttMessageBuilders.connAck()
                    .sessionPresent(false)
                    .returnCode(errorReason.getReasonCode(protocolVersion));
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return MqttAck.createSupportedAck(connAckBuilder.build());
            }
            MqttProperties properties = new MqttProperties();
            MqttProperties.StringProperty reasonStringProperty =
                    new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                            reasonString);
            properties.add(reasonStringProperty);
            connAckBuilder.properties(properties);
            return MqttAck.createSupportedAck(connAckBuilder.build());
        }
    }


    enum ErrorReason {
        IDENTIFIER_INVALID(Mqtt3ConnReasonCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                Mqtt5ConnReasonCode.CLIENT_IDENTIFIER_NOT_VALID),
        AUTH_FAILED(Mqtt3ConnReasonCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD,
                Mqtt5ConnReasonCode.BAD_USERNAME_OR_PASSWORD),
        UNSUPPORTED_VERSION(Mqtt3ConnReasonCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION,
                Mqtt5ConnReasonCode.UNSUPPORTED_PROTOCOL_VERSION
        ),
        QOS_NOT_SUPPORT(Mqtt3ConnReasonCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE,
                Mqtt5ConnReasonCode.QOS_NOT_SUPPORTED),
        WILL_QOS_NOT_SUPPORT(Mqtt3ConnReasonCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE,
                Mqtt5ConnReasonCode.QOS_NOT_SUPPORTED),
        SERVER_UNAVAILABLE(Mqtt3ConnReasonCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE,
                Mqtt5ConnReasonCode.SERVER_UNAVAILABLE),
        PROTOCOL_ERROR(Mqtt3ConnReasonCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE,
                Mqtt5ConnReasonCode.PROTOCOL_ERROR);

        private final Mqtt3ConnReasonCode v3ReasonCode;
        private final Mqtt5ConnReasonCode v5ReasonCode;

        ErrorReason(Mqtt3ConnReasonCode v3ReasonCode, Mqtt5ConnReasonCode v5ReasonCode) {
            this.v3ReasonCode = v3ReasonCode;
            this.v5ReasonCode = v5ReasonCode;
        }

        public MqttConnectReturnCode getReasonCode(int protocolVersion) {
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return v3ReasonCode.toConnectionReasonCode();
            }
            return v5ReasonCode.toConnectionReasonCode();
        }
    }
}
