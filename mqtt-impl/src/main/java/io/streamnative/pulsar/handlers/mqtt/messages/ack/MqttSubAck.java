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

import com.google.common.collect.Lists;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

public class MqttSubAck {

    public static MqttSubSuccessAckBuilder successBuilder(int protocolVersion) {
        return new MqttSubSuccessAckBuilder(protocolVersion);
    }

    public static MqttSubAck.MqttSubErrorAckBuilder errorBuilder(int protocolVersion) {
        return new MqttSubAck.MqttSubErrorAckBuilder(protocolVersion);
    }

    public final static class MqttSubSuccessAckBuilder {
        private final int protocolVersion;
        private int packetId;
        private final List<MqttQoS> grantedQoses = Lists.newArrayList();


        public MqttSubSuccessAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttSubSuccessAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttSubSuccessAckBuilder grantedQos(Collection<MqttQoS> qos) {
            grantedQoses.addAll(qos);
            return this;
        }

        public MqttSubSuccessAckBuilder addGrantedQos(MqttQoS... qos) {
            grantedQoses.addAll(Arrays.asList(qos));
            return this;
        }

        public MqttAck build() {
            return MqttAck.createSupportedAck(MqttMessageBuilders.subAck()
                    .packetId(packetId)
                    .addGrantedQoses(grantedQoses.toArray(new MqttQoS[]{}))
                    .build());
        }
    }

    public final static class MqttSubErrorAckBuilder {
        private int packetId;
        private final int protocolVersion;
        private MqttSubAck.ErrorReason errorReason;
        private String reasonString;
        private final List<MqttQoS> grantedQoses = Lists.newArrayList();

        public MqttSubErrorAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttSubAck.MqttSubErrorAckBuilder reasonString(String reasonStr) {
            this.reasonString = reasonStr;
            return this;
        }

        public MqttSubAck.MqttSubErrorAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttSubAck.MqttSubErrorAckBuilder addGrantedQos(MqttQoS... qos) {
            grantedQoses.addAll(Arrays.asList(qos));
            return this;
        }

        public MqttSubAck.MqttSubErrorAckBuilder errorReason(ErrorReason errorReason) {
            this.errorReason = errorReason;
            return this;
        }

        public MqttAck build() {
            MqttFixedHeader mqttFixedHeader =
                    new MqttFixedHeader(MqttMessageType.SUBACK, false,
                            MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessageIdAndPropertiesVariableHeader mqttSubAckVariableHeader =
                    new MqttMessageIdAndPropertiesVariableHeader(packetId, getProperties());
            List<Integer> reasonCodes = Lists.newArrayList();
            reasonCodes.addAll(grantedQoses.stream().map(MqttQoS::value).collect(Collectors.toList()));
            reasonCodes.add(errorReason.getReasonCode(protocolVersion).value());
            MqttSubAckPayload subAckPayload = new MqttSubAckPayload(reasonCodes);
            return MqttAck.createSupportedAck(new MqttSubAckMessage(mqttFixedHeader,
                    mqttSubAckVariableHeader, subAckPayload));
        }

        private MqttProperties getProperties() {
            if (!MqttUtils.isMqtt3(protocolVersion)) {
                return MqttProperties.NO_PROPERTIES;
            }
            MqttProperties properties = new MqttProperties();
            MqttPropertyUtils.setReasonString(properties, reasonString);
            return properties;
        }
    }

    @AllArgsConstructor
    public enum ErrorReason {
        AUTHORIZATION_FAIL(Mqtt3SubReasonCode.FAILURE,
                Mqtt5SubReasonCode.NOT_AUTHORIZED),
        UNSPECIFIED_ERROR(Mqtt3SubReasonCode.FAILURE,
                Mqtt5SubReasonCode.UNSPECIFIED_ERROR);

        private final Mqtt3SubReasonCode v3ReasonCode;
        private final Mqtt5SubReasonCode v5ReasonCode;

        public MqttReasonCode getReasonCode(int protocolVersion) {
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return v3ReasonCode;
            } else {
                return v5ReasonCode;
            }
        }
    }
}
