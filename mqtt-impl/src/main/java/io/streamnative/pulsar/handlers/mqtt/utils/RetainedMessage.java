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
import lombok.Getter;
import lombok.Setter;

/**
 * Retained Message.
 */
@Getter
@Setter
public class RetainedMessage {

    String topic;
    byte[] payload;
    MqttQoS qos;

    public RetainedMessage() {
    }

    public RetainedMessage(final String topic, final byte[] payload, final MqttQoS qos) {
        this.topic = topic;
        this.payload = payload;
        this.qos = qos;
    }
}
