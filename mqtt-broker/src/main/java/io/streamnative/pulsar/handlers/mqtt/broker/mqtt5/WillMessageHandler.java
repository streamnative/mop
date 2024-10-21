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
package io.streamnative.pulsar.handlers.mqtt.broker.mqtt5;

import static io.streamnative.pulsar.handlers.mqtt.common.utils.MqttMessageUtils.createMqttWillMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.mqtt.broker.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.broker.impl.MQTTSubscriptionManager;
import io.streamnative.pulsar.handlers.mqtt.broker.metric.MQTTMetricsCollector;
import io.streamnative.pulsar.handlers.mqtt.broker.qos.QosPublishHandlers;
import io.streamnative.pulsar.handlers.mqtt.common.Connection;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.common.utils.WillMessage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class WillMessageHandler {

    private final PulsarService pulsarService;
    private final MQTTSubscriptionManager mqttSubscriptionManager;
    private final MQTTConnectionManager connectionManager;
    private final String advertisedAddress;
    private final MQTTService mqttService;
    private final QosPublishHandlers qosPublishHandlers;
    private final MQTTMetricsCollector metricsCollector;

    private final ScheduledExecutorService executor;

    public WillMessageHandler(MQTTService mqttService) {
        this.mqttService = mqttService;
        this.pulsarService = mqttService.getPulsarService();
        this.mqttSubscriptionManager = mqttService.getSubscriptionManager();
        this.connectionManager = mqttService.getConnectionManager();
        this.advertisedAddress = mqttService.getPulsarService().getAdvertisedAddress();
        this.qosPublishHandlers = mqttService.getQosPublishHandlers();
        this.metricsCollector = mqttService.getMetricsCollector();
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("will-message-executor"));
    }

    private Executor delayedExecutor(long delay, TimeUnit unit) {
        // this allows CompleteableFuture to be executed with a specified delay into a scheduled executor service
        return r -> executor.schedule(r, delay, unit);
    }

    public CompletableFuture<Void> fireWillMessage(Connection connection, WillMessage willMessage) {
        if (willMessage.getDelayInterval() > 0) {
            final Executor delayed = delayedExecutor(willMessage.getDelayInterval(), TimeUnit.SECONDS);
            return CompletableFuture.runAsync(() -> sendWillMessageToPulsarTopic(connection, willMessage), delayed);
        }
        return sendWillMessageToPulsarTopic(connection, willMessage);
    }

    private CompletableFuture<Void> sendWillMessageToPulsarTopic(Connection connection, WillMessage willMessage) {
        final MqttPublishMessage msg = createMqttWillMessage(willMessage);
        final MqttAdapterMessage adapter = new MqttAdapterMessage(connection.getClientId(),
                                                                  msg, connection.isFromProxy());
        final MqttQoS qos = msg.fixedHeader().qosLevel();
        metricsCollector.addSend(msg.payload().readableBytes());
        switch (qos) {
            case AT_MOST_ONCE:
                return this.qosPublishHandlers.qos0().publish(connection, adapter);
            case AT_LEAST_ONCE:
                return this.qosPublishHandlers.qos1().publish(connection, adapter);
            case EXACTLY_ONCE:
                return this.qosPublishHandlers.qos2().publish(connection, adapter);
            default:
                log.error("[Publish] Unknown QoS-Type:{}", qos);
                return FutureUtil.failedFuture(new IllegalArgumentException("Unknown QoS-Type:" + qos));
        }
    }

    public void close() {
        this.executor.shutdown();
    }
}
