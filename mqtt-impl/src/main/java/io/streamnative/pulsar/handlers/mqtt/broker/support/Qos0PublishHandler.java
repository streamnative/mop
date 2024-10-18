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
package io.streamnative.pulsar.handlers.mqtt.broker.support;

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.common.Connection;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
/**
 * Publish handler implementation for Qos 0.
 */
@Slf4j
public class Qos0PublishHandler extends AbstractQosPublishHandler {

    public Qos0PublishHandler(MQTTService mqttService) {
        super(mqttService);
    }

    @Override
    public CompletableFuture<Void> publish(Connection connection, MqttAdapterMessage adapter) {
        final MqttPublishMessage msg = (MqttPublishMessage) adapter.getMqttMessage();
        if (MqttUtils.isRetainedMessage(msg)) {
            return retainedMessageHandler.addRetainedMessage(msg)
                .thenCompose(__ -> writeToPulsarTopic(connection, msg).thenAccept(___ -> {}));
        }
        return writeToPulsarTopic(connection, msg).thenAccept(__ -> {});
    }
}
