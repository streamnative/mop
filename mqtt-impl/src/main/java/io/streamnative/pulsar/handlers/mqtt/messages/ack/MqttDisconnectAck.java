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
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5DisConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;

public class MqttDisconnectAck {

    public static MqttDisConnectSuccessAckBuilder successBuilder(int protocolVersion) {
        return new MqttDisConnectSuccessAckBuilder(protocolVersion);
    }

    public static MqttDisconnectAck.MqttDisConnectErrorAckBuilder errorBuilder(int protocolVersion) {
        return new MqttDisconnectAck.MqttDisConnectErrorAckBuilder(protocolVersion);
    }

    public final static class MqttDisConnectSuccessAckBuilder {
        private final int protocolVersion;

        public MqttDisConnectSuccessAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }
        public MqttAck build() {
            MqttMessageBuilders.DisconnectBuilder commonBuilder = MqttMessageBuilders.disconnect();
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return MqttAck.createUnSupportAck();
            }
            return MqttAck.createSupportAck(commonBuilder.reasonCode(Mqtt5DisConnReasonCode.NORMAL.byteValue())
                    .build());
        }
    }


    public final static class MqttDisConnectErrorAckBuilder {
        private final int protocolVersion;
        private Mqtt5DisConnReasonCode reasonCode;
        private String reasonString;

        public MqttDisConnectErrorAckBuilder(int protocolVersion) {
            this.protocolVersion = protocolVersion;
        }

        public MqttDisconnectAck.MqttDisConnectErrorAckBuilder reasonString(String reasonStr) {
            this.reasonString = reasonStr;
            return this;
        }


        public MqttDisconnectAck.MqttDisConnectErrorAckBuilder reasonCode
                (Mqtt5DisConnReasonCode reasonCode) {
            this.reasonCode = reasonCode;
            return this;
        }

        public MqttAck build() {
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return MqttAck.createUnSupportAck();
            } else {
                return MqttAck.createSupportAck(MqttMessageBuilders
                        .disconnect()
                        .reasonCode(reasonCode.byteValue())
                        .properties(getProperties())
                        .build());
            }
        }

        private MqttProperties getProperties() {
            if (MqttUtils.isMqtt3(protocolVersion)) {
                return MqttProperties.NO_PROPERTIES;
            }
            MqttProperties properties = new MqttProperties();
            MqttPropertyUtils.setReasonString(properties, reasonString);
            return properties;
        }
    }
}
