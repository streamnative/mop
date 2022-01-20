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

import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;

public class MqttPubAckMessageHelper {

    public static MqttMessageBuilders.PubAckBuilder builder() {
        return MqttMessageBuilders.pubAck();
    }

    public static MqttPubAckMessageHelper.MqttPubErrorAckBuilder errorBuilder(int protocolVersion) {
        return new MqttPubAckMessageHelper.MqttPubErrorAckBuilder(protocolVersion);
    }

    public final static class MqttPubErrorAckBuilder {
        private final int protocolVersion;
        private int packetId;
        private Mqtt5PubReasonCode reasonCode;
        private String reasonString;

        public MqttPubErrorAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttPubAckMessageHelper.MqttPubErrorAckBuilder reasonString(String reasonStr) {
            this.reasonString = reasonStr;
            return this;
        }

        public MqttPubAckMessageHelper.MqttPubErrorAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public MqttPubAckMessageHelper.MqttPubErrorAckBuilder reasonCode(
                Mqtt5PubReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public MqttMessage build() {
            return MqttMessageBuilders.pubAck()
                    .reasonCode(reasonCode.byteValue())
                    .packetId(packetId)
                    .properties(getStuffedProperties())
                    .build();
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
}
