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
package io.streamnative.pulsar.handlers.mqtt.common.messages.ack;

import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.streamnative.pulsar.handlers.mqtt.common.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.common.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.common.utils.MqttUtils;

public class MqttPubAck {

    public static MqttPubSuccessAckBuilder successBuilder(int protocolVersion) {
        return new MqttPubSuccessAckBuilder(protocolVersion);
    }

    public static MqttPubAck.MqttPubErrorAckBuilder errorBuilder(int protocolVersion) {
        return new MqttPubAck.MqttPubErrorAckBuilder(protocolVersion);
    }

    public final static class MqttPubSuccessAckBuilder {
        private final int protocolVersion;
        private int packetId;
        private boolean isNoMatchingSubscription;

        public MqttPubSuccessAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttPubSuccessAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttPubSuccessAckBuilder isNoMatchingSubscription() {
            this.isNoMatchingSubscription = true;
            return this;
        }

        public MqttAck build() {
            MqttMessageBuilders.PubAckBuilder commonBuilder = MqttMessageBuilders.pubAck().packetId(packetId);
            if (MqttUtils.isMqtt3(protocolVersion)) {
                if (isNoMatchingSubscription) {
                    throw new IllegalArgumentException("MQTT3 not support [isNoMatchingSubscription]");
                }
                return MqttAck.createSupportedAck(commonBuilder.build());
            }
            if (isNoMatchingSubscription) {
                commonBuilder.reasonCode(Mqtt5PubReasonCode.NO_MATCHING_SUBSCRIBERS.byteValue());
            } else {
                commonBuilder.reasonCode(Mqtt5PubReasonCode.SUCCESS.byteValue());
            }
            return MqttAck.createSupportedAck(commonBuilder.build());
        }

    }

    public final static class MqttPubErrorAckBuilder {
        private final int protocolVersion;
        private int packetId;
        private Mqtt5PubReasonCode reasonCode;
        private String reasonString;

        private MqttProperties.UserProperties userProperties;

        public MqttPubErrorAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttPubAck.MqttPubErrorAckBuilder reasonString(String reasonStr) {
            this.reasonString = reasonStr;
            return this;
        }

        public MqttPubAck.MqttPubErrorAckBuilder userProperties(MqttProperties.UserProperties userProperties) {
            this.userProperties = userProperties;
            return this;
        }

        public MqttPubAck.MqttPubErrorAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttPubAck.MqttPubErrorAckBuilder reasonCode(
                Mqtt5PubReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public MqttAck build() {
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return MqttAck.createUnsupportedAck();
            } else {
                return MqttAck.createSupportedAck(MqttMessageBuilders.pubAck()
                        .reasonCode(reasonCode.byteValue())
                        .packetId(packetId)
                        .properties(getProperties())
                        .build());
            }
        }

        private MqttProperties getProperties() {
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return MqttProperties.NO_PROPERTIES;
            }
            MqttProperties properties = new MqttProperties();
            if (reasonString != null) {
                MqttPropertyUtils.setReasonString(properties, reasonString);
            }
            if (userProperties != null) {
                properties.add(userProperties);
            }
            return properties;
        }
    }
}
