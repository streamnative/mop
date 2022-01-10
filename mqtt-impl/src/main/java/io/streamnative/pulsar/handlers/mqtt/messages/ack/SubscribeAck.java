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

import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;
import java.util.List;
import lombok.Getter;
import org.apache.commons.compress.utils.Lists;

@Getter
public final class SubscribeAck {
    private final boolean success;
    private final int packetId;
    private final List<MqttQoS> grantedQoses;
    private final MqttSubAckMessageHelper.ErrorReason errorReason;
    private final String reasonStr;

    private SubscribeAck(boolean success, int packetId, List<MqttQoS> grantedQoses,
                         MqttSubAckMessageHelper.ErrorReason errorReason, String reasonStr) {
        this.success = success;
        this.packetId = packetId;
        this.grantedQoses = grantedQoses;
        this.errorReason = errorReason;
        this.reasonStr = reasonStr;
    }

    public static SubscribeAckBuilder success() {
        return new SubscribeAckBuilder().success(true);
    }

    public static SubscribeAckBuilder error() {
        return new SubscribeAckBuilder().success(false);
    }

    public static final class SubscribeAckBuilder {
        private boolean success;
        private int packetId;
        private final List<MqttQoS> grantedQoses = Lists.newArrayList();
        private MqttSubAckMessageHelper.ErrorReason errorReason;
        private String reasonStr;

        public SubscribeAckBuilder success(boolean success) {
            this.success = success;
            return this;
        }

        public SubscribeAckBuilder packetId(int packetId) {
            this.packetId = packetId;
            return this;
        }

        public SubscribeAckBuilder grantedQoses(List<MqttQoS> qoses) {
            grantedQoses.addAll(qoses);
            return this;
        }

        public SubscribeAckBuilder errorReason(MqttSubAckMessageHelper.ErrorReason errorReason) {
            this.errorReason = errorReason;
            return this;
        }

        public SubscribeAckBuilder reasonStr(String reasonStr) {
            this.reasonStr = reasonStr;
            return this;
        }

        public SubscribeAck build() {
            return new SubscribeAck(success, packetId, grantedQoses, errorReason, reasonStr);
        }

    }
}
