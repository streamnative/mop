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

import static io.netty.handler.codec.mqtt.MqttMessageType.PUBACK;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.AbstractQosPublishHandler;
import io.streamnative.pulsar.handlers.mqtt.Connection;
import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.exception.handler.GlobalExceptionHandler;
import io.streamnative.pulsar.handlers.mqtt.messages.handler.ProtocolAckHandler;
import io.streamnative.pulsar.handlers.mqtt.messages.handler.ProtocolAckHandlerHelper;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;
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
        ProtocolAckHandler ackHandler =
                ProtocolAckHandlerHelper.getAndCheckByProtocolVersion(channel);
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
                    GlobalExceptionHandler.handleServerException(channel, cause);
                    // decrement server receive publish message counter
                    connection.decrementServerReceivePubMessage();
                    return;
                }
                GlobalExceptionHandler.handleServerException(channel, cause)
                        .orElseHandleCommon(PUBACK, packetId);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] Write {} to Pulsar topic succeed.", topic, msg);
            }
            ackHandler.pubOk(connection, topic, packetId);
        });
    }
}
