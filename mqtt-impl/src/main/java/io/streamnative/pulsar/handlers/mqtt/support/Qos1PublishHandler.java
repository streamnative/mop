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
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.messages.ack.PublishAck;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.support.handler.AckHandler;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Publish handler implementation for Qos 1.
 */
@Slf4j
public class Qos1PublishHandler extends AbstractQosPublishHandler {

    public Qos1PublishHandler(PulsarService pulsarService, MQTTServerConfiguration configuration, Channel channel) {
        super(pulsarService, configuration, channel);
    }

    @Override
    public CompletableFuture<Void> publish(MqttPublishMessage msg) {
        final Connection connection = NettyUtils.getConnection(channel);
        final int protocolVersion = connection.getProtocolVersion();
        final boolean isMqtt5 = MqttUtils.isMqtt5(protocolVersion);
        final int packetId = msg.variableHeader().packetId();
        final String topic = msg.variableHeader().topicName();
        // we need to check if subscription exist when protocol version is mqtt 5.x
        return writeToPulsarTopic(msg, isMqtt5)
                .thenCompose(__ -> {
                    PublishAck publishAck = PublishAck.builder()
                            .success(true)
                            .packetId(packetId)
                            .build();
                    CompletableFuture<Void> publishAckFuture = new CompletableFuture<>();
                    connection.getAckHandler().sendPublishAck(connection, publishAck)
                            .addListener(result -> {
                                if (result.isSuccess()) {
                                    // decrement server receive publish message counter
                                    connection.decrementServerReceivePubMessage();
                                    if (log.isDebugEnabled()) {
                                        log.debug("[{}] Send Pub Ack {} to {}", topic, msg.variableHeader().packetId(),
                                                connection.getClientId());
                                    }
                                    publishAckFuture.complete(null);
                                } else {
                                    log.warn("[{}] Failed to send Pub Ack {} to {}", topic,
                                            msg.variableHeader().packetId(), connection.getClientId(), result.cause());
                                    publishAckFuture.completeExceptionally(result.cause());
                                }
                            });
                    return publishAckFuture;
                }).exceptionally(ex -> {
                    Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                    AckHandler ackHandler = connection.getAckHandler();
                    if (realCause instanceof MQTTNoMatchingSubscriberException) {
                        log.warn("[{}] Write {} to Pulsar topic succeed. But do not have subscriber.", topic, msg);
                        PublishAck noMatchingSubscribersAck = PublishAck.builder()
                                .success(true)
                                .packetId(packetId)
                                .reasonCode(Mqtt5PubReasonCode.NO_MATCHING_SUBSCRIBERS)
                                .build();
                        ackHandler.sendPublishAck(connection, noMatchingSubscribersAck)
                                .addListener(__ -> connection.decrementServerReceivePubMessage());
                    } else if (realCause instanceof BrokerServiceException.TopicNotFoundException) {
                        log.warn("Topic [{}] Not found, the configuration [isAllowAutoTopicCreation={}]",
                                topic, pulsarService.getConfig().isAllowAutoTopicCreation());
                        PublishAck topicNotFoundAck = PublishAck.builder()
                                .success(false)
                                .packetId(packetId)
                                .reasonCode(Mqtt5PubReasonCode.UNSPECIFIED_ERROR)
                                .reasonString("Topic not found")
                                .build();
                        ackHandler.sendPublishAck(connection, topicNotFoundAck);
                    } else {
                        log.error("[{}] Publish msg {} fail.", topic, msg, ex);
                        PublishAck unKnowErrorAck = PublishAck.builder()
                                .success(false)
                                .packetId(packetId)
                                .reasonCode(Mqtt5PubReasonCode.UNSPECIFIED_ERROR)
                                .reasonString(realCause.getMessage())
                                .build();
                        ackHandler.sendPublishAck(connection, unKnowErrorAck);
                    }
                    return null;
                });
    }
}
