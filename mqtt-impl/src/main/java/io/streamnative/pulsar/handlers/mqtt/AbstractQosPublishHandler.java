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
package io.streamnative.pulsar.handlers.mqtt;

import static io.streamnative.pulsar.handlers.mqtt.utils.PulsarMessageConverter.toPulsarMsg;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.utils.MessagePublishContext;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Abstract class for publish handler.
 */
public abstract class AbstractQosPublishHandler implements QosPublishHandler {

    protected final PulsarService pulsarService;
    protected final MQTTServerConfiguration configuration;
    protected final ConnectionDescriptorStore connectionDescriptors;

    protected AbstractQosPublishHandler(PulsarService pulsarService, MQTTServerConfiguration configuration,
                                        ConnectionDescriptorStore connectionDescriptors) {
        this.pulsarService = pulsarService;
        this.configuration = configuration;
        this.connectionDescriptors = connectionDescriptors;
    }

    protected CompletableFuture<Optional<Topic>> getTopicReference(MqttPublishMessage msg) {
        return PulsarTopicUtils.getTopicReference(pulsarService, msg.variableHeader().topicName(),
                configuration.getDefaultTenant(), configuration.getDefaultNamespace());
    }

    protected CompletableFuture<PositionImpl> writeToPulsarTopic(MqttPublishMessage msg) {
        return getTopicReference(msg).thenCompose(topicOp ->
                topicOp.map(topic -> MessagePublishContext.publishMessages(toPulsarMsg(msg), topic))
                .orElseGet(() -> FutureUtil.failedFuture(
                new BrokerServiceException.TopicNotFoundException(msg.variableHeader().topicName()))));
    }
}
