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

import com.google.common.collect.Lists;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
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
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

public class MqttSubAckMessageHelper {

    public static MqttMessageBuilders.SubAckBuilder builder() {
        return MqttMessageBuilders.subAck();
    }

    public static MqttSubAckMessageHelper.MqttSubErrorAckBuilder errorBuilder(int protocolVersion) {
        return new MqttSubAckMessageHelper.MqttSubErrorAckBuilder(protocolVersion);
    }

    public static class MqttSubErrorAckBuilder {
        private int packetId;
        private final int protocolVersion;
        private MqttSubAckMessageHelper.ErrorReason errorReason;
        private String reasonString;
        private final List<MqttQoS> grantedQoses = Lists.newArrayList();

        public MqttSubErrorAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttSubAckMessageHelper.MqttSubErrorAckBuilder reasonString(String reasonStr) {
            this.reasonString = reasonStr;
            return this;
        }

        public MqttSubAckMessageHelper.MqttSubErrorAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttSubAckMessageHelper.MqttSubErrorAckBuilder addGrantedQos(MqttQoS... qos) {
            grantedQoses.addAll(Arrays.asList(qos));
            return this;
        }

        public MqttSubAckMessageHelper.MqttSubErrorAckBuilder errorReason(ErrorReason errorReason) {
            this.errorReason = errorReason;
            return this;
        }

        public MqttMessage build() {
            MqttFixedHeader mqttFixedHeader =
                    new MqttFixedHeader(MqttMessageType.SUBACK, false,
                            MqttQoS.AT_MOST_ONCE, false, 0);
            MqttMessageIdAndPropertiesVariableHeader mqttSubAckVariableHeader =
                    new MqttMessageIdAndPropertiesVariableHeader(packetId, getStuffedProperties());
            List<Integer> reasonCodes = Lists.newArrayList();
            reasonCodes.addAll(grantedQoses.stream().map(MqttQoS::value).collect(Collectors.toList()));
            reasonCodes.add(errorReason.getReasonCode(protocolVersion).value());
            MqttSubAckPayload subAckPayload = new MqttSubAckPayload(reasonCodes);
            return new MqttSubAckMessage(mqttFixedHeader, mqttSubAckVariableHeader, subAckPayload);
        }

        private MqttProperties getStuffedProperties() {
            if (MqttUtils.isMqtt5(protocolVersion)) {
                MqttProperties properties = new MqttProperties();
                MqttPropertyUtils.stuffReasonString(properties, reasonString);
                return properties;
            } else {
                return MqttProperties.NO_PROPERTIES;
            }
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
            if (MqttUtils.isMqtt5(protocolVersion)) {
                return v5ReasonCode;
            } else {
                return v3ReasonCode;
            }
        }
    }
}
