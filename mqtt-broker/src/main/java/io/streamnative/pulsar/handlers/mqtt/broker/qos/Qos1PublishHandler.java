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

import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.broker.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.common.Connection;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.common.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.common.exception.MQTTTopicAliasExceedsLimitException;
import io.streamnative.pulsar.handlers.mqtt.common.exception.MQTTTopicAliasNotFoundException;
import io.streamnative.pulsar.handlers.mqtt.common.messages.ack.MqttAck;
import io.streamnative.pulsar.handlers.mqtt.common.messages.ack.MqttPubAck;
import io.streamnative.pulsar.handlers.mqtt.common.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.common.utils.MqttUtils;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Publish handler implementation for Qos 1.
 */
@Slf4j
public class Qos1PublishHandler extends AbstractQosPublishHandler {

    public Qos1PublishHandler(MQTTService mqttService) {
        super(mqttService);
    }

    @Override
    public CompletableFuture<Void> publish(Connection connection, MqttAdapterMessage adapter) {
        final MqttPublishMessage msg = (MqttPublishMessage) adapter.getMqttMessage();
        final int protocolVersion = connection.getProtocolVersion();
        final int packetId = msg.variableHeader().packetId();
        final String topic = msg.variableHeader().topicName();
        final CompletableFuture<Void> ret;
        if (MqttUtils.isRetainedMessage(msg)) {
            ret = retainedMessageHandler.addRetainedMessage(msg)
                .thenCompose(__ -> writeToPulsarTopic(connection,
                        msg, !MqttUtils.isMqtt3(protocolVersion)).thenApply(___ -> null));
        } else {
            ret = writeToPulsarTopic(connection, msg, !MqttUtils.isMqtt3(protocolVersion)).thenApply(__ -> null);
        }
        // we need to check if subscription exist when protocol version is mqtt 5.x
        return ret
                .thenCompose(__ -> {
                    MqttAck pubAck = MqttPubAck.successBuilder(protocolVersion)
                            .packetId(packetId)
                            .build();
                    CompletableFuture<Void> future = connection.sendAck(pubAck);
                    future.whenComplete((result, error) -> {
                        connection.decrementServerReceivePubMessage();
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Send Pub Ack {} to {}", topic, msg.variableHeader().packetId(),
                                    connection.getClientId());
                        }
                    });
                    return future;
                }).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    if (realCause instanceof MQTTNoMatchingSubscriberException) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Write {} to Pulsar topic succeed. But do not have subscriber.", topic, msg);
                        }
                        MqttAck mqttAck = MqttPubAck.successBuilder(protocolVersion)
                                .packetId(packetId)
                                .isNoMatchingSubscription()
                                .build();
                        connection.sendAck(mqttAck)
                                .whenComplete((result, error)->{
                                    connection.decrementServerReceivePubMessage();
                                });
                    } else if (realCause instanceof BrokerServiceException.TopicNotFoundException) {
                        log.warn("Topic [{}] Not found, the configuration [isAllowAutoTopicCreation={}]",
                                topic, pulsarService.getConfig().isAllowAutoTopicCreation());
                        MqttPubAck.MqttPubErrorAckBuilder pubAckBuilder = MqttPubAck.errorBuilder(protocolVersion)
                                .packetId(packetId)
                                .reasonCode(Mqtt5PubReasonCode.UNSPECIFIED_ERROR);
                        if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                            pubAckBuilder.reasonString("Topic not found");
                        }
                        connection.sendAckThenClose(pubAckBuilder.build());
                    } else if (realCause instanceof BrokerServiceException.ServiceUnitNotReadyException) {
                        String errorMsg = String.format("[%s] Publish message fail,"
                                        + " because the topic is not served by this broker", topic);
                        log.error(errorMsg);
                        MqttProperties.UserProperties userProperties = new MqttProperties.UserProperties();
                        userProperties.add("topicName", topic);
                        MqttPubAck.MqttPubErrorAckBuilder pubAckBuilder = MqttPubAck.errorBuilder(protocolVersion)
                                .packetId(packetId)
                                .reasonString(errorMsg)
                                .userProperties(userProperties)
                                .reasonCode(Mqtt5PubReasonCode.UNSPECIFIED_ERROR);
                        connection.sendAck(pubAckBuilder.build());
                    } else if (realCause instanceof MQTTTopicAliasNotFoundException) {
                        log.error("[{}] Publish message fail {}, because the topic alias {} not found.", topic, msg,
                                ((MQTTTopicAliasNotFoundException) realCause).getAlias(), ex);
                        MqttPubAck.MqttPubErrorAckBuilder pubAckBuilder = MqttPubAck.errorBuilder(protocolVersion)
                                .packetId(packetId)
                                .reasonCode(Mqtt5PubReasonCode.TOPIC_NAME_INVALID);
                        if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                            pubAckBuilder.reasonString(ex.getMessage());
                        }
                        connection.sendAckThenClose(pubAckBuilder.build());
                    } else if (realCause instanceof MQTTTopicAliasExceedsLimitException) {
                        log.error("[{}] Publish message fail {},"
                                        + " because the topic alias {} is exceed topic alias maximum {}.", topic, msg,
                                ((MQTTTopicAliasExceedsLimitException) realCause).getAlias(),
                                ((MQTTTopicAliasExceedsLimitException) realCause).getTopicAliasMaximum(), ex);
                        MqttPubAck.MqttPubErrorAckBuilder pubAckBuilder = MqttPubAck.errorBuilder(protocolVersion)
                                .packetId(packetId)
                                .reasonCode(Mqtt5PubReasonCode.QUOTA_EXCEEDED);
                        if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                            pubAckBuilder.reasonString(ex.getMessage());
                        }
                        connection.sendAckThenClose(pubAckBuilder.build());
                    } else {
                        log.error("[{}] Publish msg {} fail.", topic, msg, ex);
                        MqttPubAck.MqttPubErrorAckBuilder pubAckBuilder = MqttPubAck.errorBuilder(protocolVersion)
                                .packetId(packetId)
                                .reasonCode(Mqtt5PubReasonCode.UNSPECIFIED_ERROR);
                        if (connection.getClientRestrictions().isAllowReasonStrOrUserProperty()) {
                            pubAckBuilder.reasonString(realCause.getMessage());
                        }
                        connection.sendAckThenClose(pubAckBuilder.build());
                    }
                    return null;
                });
    }
}
