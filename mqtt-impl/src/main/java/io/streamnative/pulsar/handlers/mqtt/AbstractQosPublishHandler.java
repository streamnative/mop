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
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTTopicAliasExceedsLimitException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTTopicAliasNotFoundException;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.support.RetainedMessageHandler;
import io.streamnative.pulsar.handlers.mqtt.utils.MessagePublishContext;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
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
    protected final RetainedMessageHandler retainedMessageHandler;
    protected final MQTTServerConfiguration configuration;

    protected AbstractQosPublishHandler(MQTTService mqttService) {
        this.pulsarService = mqttService.getPulsarService();
        this.retainedMessageHandler = mqttService.getRetainedMessageHandler();
        this.configuration = mqttService.getServerConfiguration();
    }

    protected CompletableFuture<Optional<Topic>> getTopicReference(String mqttTopicName) {
        return PulsarTopicUtils.getTopicReference(pulsarService, mqttTopicName,
                configuration.getDefaultTenant(), configuration.getDefaultNamespace(), true
                , configuration.getDefaultTopicDomain())
                .exceptionally(ex -> {
                    final Throwable rc = FutureUtil.unwrapCompletionException(ex);
                    if (rc instanceof IllegalStateException) {
                        // convert namespace bundle is being unloaded exception to service unit not ready.
                        throw FutureUtil.wrapToCompletionException(
                                new BrokerServiceException.ServiceUnitNotReadyException(rc.getMessage()));
                    }
                    throw FutureUtil.wrapToCompletionException(rc);
                });
    }

    protected CompletableFuture<PositionImpl> writeToPulsarTopic(TopicAliasManager topicAliasManager,
                                                                 MqttPublishMessage msg) {
        return writeToPulsarTopic(topicAliasManager, msg, false);
    }

    /**
     * Convert mqtt protocol message to pulsar message and send it.
     * @param msg                    MQTT protocol message
     * @param checkSubscription Check if the subscription exists, throw #{MQTTNoMatchingSubscriberException}
     *                              if the subscription does not exist;
     */
    protected CompletableFuture<PositionImpl> writeToPulsarTopic(TopicAliasManager topicAliasManager,
                                                                 MqttPublishMessage msg, boolean checkSubscription) {
        Optional<Integer> topicAlias = MqttPropertyUtils.getProperty(msg.variableHeader().properties(),
                MqttProperties.MqttPropertyType.TOPIC_ALIAS);
        String mqttTopicName;
        if (topicAlias.isPresent()) {
            int alias = topicAlias.get();
            String tpName = msg.variableHeader().topicName();
            if (StringUtils.isNoneBlank(tpName)) {
                // update alias
                boolean updateSuccess = topicAliasManager.updateTopicAlias(tpName, alias);
                if (!updateSuccess) {
                    throw new MQTTTopicAliasExceedsLimitException(alias,
                            topicAliasManager.getTopicMaximumAlias());
                }
            }
            Optional<String> realName = topicAliasManager.getTopicByAlias(alias);
            if (!realName.isPresent()) {
                throw new MQTTTopicAliasNotFoundException(alias);
            }
            mqttTopicName = realName.get();
        } else {
            mqttTopicName = msg.variableHeader().topicName();
        }
        return getTopicReference(mqttTopicName).thenCompose(topicOp -> topicOp.map(topic -> {
            MessageImpl<byte[]> message = toPulsarMsg(configuration, topic, msg.variableHeader().properties(),
                    msg.payload().nioBuffer());
            CompletableFuture<PositionImpl> ret = MessagePublishContext.publishMessages(message, topic);
            message.recycle();
            return ret.thenApply(position -> {
                if (checkSubscription && topic.getSubscriptions().isEmpty()) {
                    throw new MQTTNoMatchingSubscriberException(mqttTopicName);
                }
                return position;
            });
        }).orElseGet(() -> FutureUtil.failedFuture(
                new BrokerServiceException.TopicNotFoundException(mqttTopicName))));
    }
}
