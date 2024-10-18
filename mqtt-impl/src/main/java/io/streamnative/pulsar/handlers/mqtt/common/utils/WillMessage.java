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
package io.streamnative.pulsar.handlers.mqtt.common.utils;

import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.List;
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
    List<MqttProperties.StringPair> userProperty;

    String contentType;

    String responseTopic;

    String correlationData;

    int payloadFormatIndicator;

    int messageExpiryInterval;

    int delayInterval;

    public WillMessage() {
    }

    public WillMessage(final String topic,
                       final byte[] willMessage,
                       final MqttQoS qos,
                       final boolean retained,
                       final List<MqttProperties.StringPair> userProperty,
                       final String contentType,
                       final String responseTopic,
                       final String correlationData,
                       final int payloadFormatIndicator,
                       final int messageExpiryInterval,
                       final int delayInterval) {
        this.topic = topic;
        this.willMessage = willMessage;
        this.qos = qos;
        this.retained = retained;
        this.userProperty = userProperty;
        this.contentType = contentType;
        this.responseTopic = responseTopic;
        this.correlationData = correlationData;
        this.payloadFormatIndicator = payloadFormatIndicator;
        this.messageExpiryInterval = messageExpiryInterval;
        this.delayInterval = delayInterval;
    }
}
