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
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.AbstractQosPublishHandler;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * Publish handler implementation for Qos 2.
 */
@Slf4j
public class Qos2PublishHandler extends AbstractQosPublishHandler {

    public Qos2PublishHandler(MQTTService mqttService, MQTTServerConfiguration configuration, Channel channel) {
        super(mqttService, configuration, channel);
    }

    @Override
    public CompletableFuture<Void> publish(MqttAdapterMessage adapter) {
        final MqttPublishMessage msg = (MqttPublishMessage) adapter.getMqttMessage();
        log.error("[{}] Failed to write data due to QoS2 does not support.", msg.variableHeader().topicName());
        channel.close();
        return CompletableFuture.completedFuture(null);
    }
}
