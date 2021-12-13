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
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.AbstractQosPublishHandler;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.exception.handler.MopExceptionHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttPubAckMessageHelper;
import io.streamnative.pulsar.handlers.mqtt.messages.handler.ProtocolAckHandler;
import io.streamnative.pulsar.handlers.mqtt.messages.handler.ProtocolAckHandlerHelper;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;

/**
 * Publish handler implementation for Qos 1.
 */
@Slf4j
public class Qos1PublishHandler extends AbstractQosPublishHandler {

    public Qos1PublishHandler(PulsarService pulsarService, MQTTServerConfiguration configuration) {
        super(pulsarService, configuration);
    }

    @Override
    public void publish(Channel channel, MqttPublishMessage msg) {
        Optional<ProtocolAckHandler> ackHandler =
                ProtocolAckHandlerHelper.getAndCheckByProtocolVersion(channel);
        if (!ackHandler.isPresent()){
            return;
        }
        Connection connection = NettyUtils.getConnection(channel);
        int protocolVersion = NettyUtils.getProtocolVersion(channel);
        final boolean isMqtt5 = MqttUtils.isMqtt5(protocolVersion);
        int packetId = msg.variableHeader().packetId();
        final String topic = msg.variableHeader().topicName();
        // Support mqtt 5 version.
        CompletableFuture<PositionImpl> writeToPulsarResultFuture =
                isMqtt5 ? writeToPulsarTopicAndCheckIfSubscriptionMatching(msg) : writeToPulsarTopic(msg);
        writeToPulsarResultFuture.whenComplete((p, e) -> {
            if (e != null) {
                Throwable cause = e.getCause();
                if (cause instanceof MQTTNoMatchingSubscriberException) {
                    log.warn("[{}] Write {} to Pulsar topic succeed. But do not have subscriber.", topic, msg);
                    MqttMessage unspecifiedErrorPubAckMessage =
                            MqttPubAckMessageHelper.createMqtt5(packetId, Mqtt5PubReasonCode.NO_MATCHING_SUBSCRIBERS,
                                    cause.getMessage());
                    channel.writeAndFlush(unspecifiedErrorPubAckMessage);
                    // decrement server receive publish message counter
                    connection.decrementServerReceivePubMessage();
                    return;
                }
                log.error("[{}] Write {} to Pulsar topic failed.", topic, msg, e);
                MopExceptionHelper.handle(MqttMessageType.PUBLISH, packetId, channel, cause);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] Write {} to Pulsar topic succeed.", topic, msg);
            }
            ackHandler.get().pubOk(connection, topic, packetId);
        });
    }
}
