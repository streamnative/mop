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
package io.streamnative.pulsar.handlers.mqtt.utils;

import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.Arrays;
import lombok.Getter;
import lombok.Setter;

/**
 * Will Message.
 */
@Getter
@Setter
public class WillMessage {

    String topic;
    byte[] willMessage;
    MqttQoS qos;
    boolean retained;

    public WillMessage() {
    }

    public WillMessage(final String topic, final byte[] willMessage, final MqttQoS qos, final boolean retained) {
        this.topic = topic;
        this.willMessage = Arrays.copyOf(willMessage, willMessage.length);
        this.qos = qos;
        this.retained = retained;
    }
}
