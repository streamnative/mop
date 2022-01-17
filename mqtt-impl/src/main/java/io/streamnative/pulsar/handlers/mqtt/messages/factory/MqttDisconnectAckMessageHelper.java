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
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;

public class MqttDisconnectAckMessageHelper {

    public static MqttMessageBuilders.DisconnectBuilder builder() {
        return MqttMessageBuilders.disconnect();
    }

    public static MqttDisconnectAckMessageHelper.MqttDisConnectErrorAckBuilder errorBuilder(int protocolVersion) {
        return new MqttDisconnectAckMessageHelper.MqttDisConnectErrorAckBuilder(protocolVersion);
    }

    public static class MqttDisConnectErrorAckBuilder {
        private final int protocolVersion;
        private Mqtt5DisConnReasonCode errorReason;
        private String reasonString;

        public MqttDisConnectErrorAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttDisconnectAckMessageHelper.MqttDisConnectErrorAckBuilder reasonString(String reasonStr) {
            this.reasonString = reasonStr;
            return this;
        }


        public MqttDisconnectAckMessageHelper.MqttDisConnectErrorAckBuilder errorReason
                (Mqtt5DisConnReasonCode errorReason) {
            this.errorReason = errorReason;
            return this;
        }

        public MqttMessage build() {
            return MqttMessageBuilders
                    .disconnect()
                    .reasonCode(errorReason.byteValue())
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
