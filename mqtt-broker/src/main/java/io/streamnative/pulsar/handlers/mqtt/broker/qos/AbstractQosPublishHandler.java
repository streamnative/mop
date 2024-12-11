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
package io.streamnative.pulsar.handlers.mqtt.broker.qos;

import static io.streamnative.pulsar.handlers.mqtt.broker.impl.PulsarMessageConverter.toPulsarMsg;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.broker.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.broker.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.broker.impl.consumer.MessagePublishContext;
import io.streamnative.pulsar.handlers.mqtt.common.Connection;
import io.streamnative.pulsar.handlers.mqtt.common.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.common.exception.MQTTTopicAliasExceedsLimitException;
import io.streamnative.pulsar.handlers.mqtt.common.exception.MQTTTopicAliasNotFoundException;
import io.streamnative.pulsar.handlers.mqtt.common.messages.MqttPropertyUtils;
import io.streamnative.pulsar.handlers.mqtt.common.mqtt5.RetainedMessageHandler;
import io.streamnative.pulsar.handlers.mqtt.common.mqtt5.TopicAliasManager;
import io.streamnative.pulsar.handlers.mqtt.common.utils.PulsarTopicUtils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Abstract class for publish handler.
 */
@Slf4j
public abstract class AbstractQosPublishHandler implements QosPublishHandler {

    protected final PulsarService pulsarService;
    protected final RetainedMessageHandler retainedMessageHandler;
    protected final MQTTServerConfiguration configuration;
    private final ConcurrentHashMap<String, Long> sequenceIdMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Producer> producerMap = new ConcurrentHashMap<>();


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

    protected CompletableFuture<Position> writeToPulsarTopic(Connection connection, MqttPublishMessage msg) {
        return writeToPulsarTopic(connection, msg, false);
    }

    /**
     * Convert mqtt protocol message to pulsar message and send it.
     * @param msg                    MQTT protocol message
     * @param checkSubscription Check if the subscription exists, throw #{MQTTNoMatchingSubscriberException}
     *                              if the subscription does not exist;
     */
    protected CompletableFuture<Position> writeToPulsarTopic(Connection connection, MqttPublishMessage msg,
                                                                 boolean checkSubscription) {
        TopicAliasManager topicAliasManager = connection.getTopicAliasManager();
        String producerName = connection.getClientId();
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
            long lastPublishedSequenceId = -1;
            Producer producer = producerMap.compute(producerName, (k, v) -> {
                if (v == null) {
                    v = MQTTProducer.create(topic, connection.getServerCnx(), producerName);
                    final CompletableFuture<Optional<Long>> producerFuture =
                            topic.addProducer(v, new CompletableFuture<>());
                    producerFuture.whenComplete((r, e) -> {
                        if (e != null) {
                            log.error("Failed to add producer", e);
                        }
                    });
                }
                return v;
            });
            if (topic instanceof PersistentTopic) {
                final long lastPublishedId = ((PersistentTopic) topic).getLastPublishedSequenceId(producerName);
                lastPublishedSequenceId = sequenceIdMap.compute(producerName, (k, v) -> {
                    long id;
                    if (v == null) {
                        id = lastPublishedId + 1;
                    } else {
                        id = Math.max(v, lastPublishedId) + 1;
                    }
                    return id;
                });
            }
            final ByteBuf payload = msg.payload();
            MessageImpl<byte[]> message = toPulsarMsg(configuration, topic, msg.variableHeader().properties(),
                    payload.nioBuffer());
            CompletableFuture<Position> ret = MessagePublishContext.publishMessages(producerName, message,
                    lastPublishedSequenceId, topic);
            message.recycle();
            return ret.thenApply(position -> {
                topic.incrementPublishCount(producer, 1, payload.readableBytes());
                if (checkSubscription && topic.getSubscriptions().isEmpty()) {
                    throw new MQTTNoMatchingSubscriberException(mqttTopicName);
                }
                return position;
            });
        }).orElseGet(() -> FutureUtil.failedFuture(
                new BrokerServiceException.TopicNotFoundException(mqttTopicName))));
    }

    @Override
    public void closeProducer(Connection connection) {
        final Producer producer = producerMap.remove(connection.getClientId());
        if (producer != null) {
            producer.close(true);
        }
    }
}
