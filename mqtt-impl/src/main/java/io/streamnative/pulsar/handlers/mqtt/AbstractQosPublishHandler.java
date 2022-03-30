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
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.utils.MessagePublishContext;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Abstract class for publish handler.
 */
public abstract class AbstractQosPublishHandler implements QosPublishHandler {

    protected final PulsarService pulsarService;
    protected final MQTTServerConfiguration configuration;
    protected final Channel channel;

    protected AbstractQosPublishHandler(PulsarService pulsarService, MQTTServerConfiguration configuration,
                                        Channel channel) {
        this.pulsarService = pulsarService;
        this.configuration = configuration;
        this.channel = channel;
    }

    protected CompletableFuture<Optional<Topic>> getTopicReference(MqttPublishMessage msg) {
        return PulsarTopicUtils.getTopicReference(pulsarService, msg.variableHeader().topicName(),
                configuration.getDefaultTenant(), configuration.getDefaultNamespace(), true
                , configuration.getDefaultTopicDomain());
    }

    protected CompletableFuture<PositionImpl> writeToPulsarTopic(MqttPublishMessage msg) {
        return writeToPulsarTopic(msg, false);
    }

    /**
     * Convert mqtt protocol message to pulsar message and send it.
     * @param msg                    MQTT protocol message
     * @param checkSubscription Check if the subscription exists, throw #{MQTTNoMatchingSubscriberException}
     *                              if the subscription does not exist;
     */
    protected CompletableFuture<PositionImpl> writeToPulsarTopic(MqttPublishMessage msg,
                                                                 boolean checkSubscription) {
        return getTopicReference(msg).thenCompose(topicOp -> topicOp.map(topic -> {
            MessageImpl<byte[]> message = toPulsarMsg(topic, msg);
            CompletableFuture<PositionImpl> ret = MessagePublishContext.publishMessages(message, topic);
            message.recycle();
            return ret.thenApply(position -> {
                if (checkSubscription && topic.getSubscriptions().isEmpty()) {
                    throw new MQTTNoMatchingSubscriberException(msg.variableHeader().topicName());
                }
                return position;
            });
        }).orElseGet(() -> FutureUtil.failedFuture(
                new BrokerServiceException.TopicNotFoundException(msg.variableHeader().topicName()))));
    }
}
