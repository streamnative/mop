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
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5UnsubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;

public class MqttUnsubAck {

    public static MqttUnsubSuccessAckBuilder successBuilder(int protocolVersion) {
        return new MqttUnsubSuccessAckBuilder(protocolVersion);
    }

    public static MqttUnsubAck.MqttUnsubErrorAckBuilder errorBuilder(int protocolVersion) {
        return new MqttUnsubAck.MqttUnsubErrorAckBuilder(protocolVersion);
    }

    public final static class MqttUnsubSuccessAckBuilder {
        private final int protocolVersion;
        private int packetId;
        private boolean isNoSubscriptionExisted;

        public MqttUnsubSuccessAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttUnsubSuccessAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttUnsubSuccessAckBuilder isNoSubscriptionExisted() {
            this.isNoSubscriptionExisted = true;
            return this;
        }

        public MqttAck build() {
            MqttMessageBuilders.UnsubAckBuilder commonBuilder =
                    MqttMessageBuilders.unsubAck().packetId(packetId);
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return MqttAck.createSupportAck(commonBuilder.build());
            }
            if (isNoSubscriptionExisted) {
                commonBuilder.addReasonCode(Mqtt5UnsubReasonCode.NO_SUBSCRIPTION_EXISTED.byteValue());
            } else {
                commonBuilder.addReasonCode(Mqtt5UnsubReasonCode.SUCCESS.byteValue());
            }
            return MqttAck.createSupportAck(commonBuilder.build());
        }

    }


    public final static class MqttUnsubErrorAckBuilder {
        private final int protocolVersion;
        private int packetId;
        private Mqtt5UnsubReasonCode reasonCode;
        private String reasonString;

        public MqttUnsubErrorAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttUnsubAck.MqttUnsubErrorAckBuilder reasonString(String reasonStr) {
            this.reasonString = reasonStr;
            return this;
        }

        public MqttUnsubAck.MqttUnsubErrorAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttUnsubAck.MqttUnsubErrorAckBuilder reasonCode(Mqtt5UnsubReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public MqttAck build() {
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return MqttAck.createUnSupportAck();
            }
            return MqttAck.createSupportAck(MqttMessageBuilders.unsubAck()
                    .packetId(packetId)
                    .addReasonCode(reasonCode.shortValue())
                    .properties(getProperties())
                    .build());
        }

        private MqttProperties getProperties() {
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return MqttProperties.NO_PROPERTIES;
            }
            MqttProperties properties = new MqttProperties();
            MqttPropertyUtils.stuffReasonString(properties, reasonString);
            return properties;
        }

    }

}
