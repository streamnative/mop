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
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTTopicNotExistedException;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttUnsubAckMessageFactory;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttUnsubAckReasonCode;

/**
 * MQTT version 5.0 exception handler.
 * use this handler to implement reason code.
 */
public class MQTT5ExceptionHandler {

    /**
     * handle ubSubscribe Exception.
     *
     * @param messageID - Mqtt message exception
     * @param channel   - Netty Nio channel
     * @param ex        - exception
     */
    public static void handleUnSubscribeException(int messageID, Channel channel, Throwable ex) {
        if (ex.getCause() instanceof MQTTNoSubscriptionExistedException) {
            MqttMessage unSubscriptionExistedAckMessage = MqttUnsubAckMessageFactory.createMqtt5(messageID,
                    MqttUnsubAckReasonCode.NO_SUBSCRIPTION_EXISTED, ex.getCause().getMessage());
            channel.writeAndFlush(unSubscriptionExistedAckMessage);
        } else if (ex.getCause() instanceof MQTTServerException) {
            MqttMessage implementationSpecificErrorAckMessage =
                    MqttUnsubAckMessageFactory.createMqtt5(messageID,
                            MqttUnsubAckReasonCode.IMPLEMENTATION_SPECIFIC_ERROR, ex.getCause().getMessage());
            channel.writeAndFlush(implementationSpecificErrorAckMessage);
        } else if (ex.getCause() instanceof MQTTTopicNotExistedException) {
            MqttMessage topicFilterInvalidAckMessage =
                    MqttUnsubAckMessageFactory.createMqtt5(messageID,
                            MqttUnsubAckReasonCode.TOPIC_FILTER_INVALID, ex.getCause().getMessage());
            channel.writeAndFlush(topicFilterInvalidAckMessage);
        } else {
            MqttMessage unSpecifiedError = MqttUnsubAckMessageFactory.createMqtt5(messageID,
                    MqttUnsubAckReasonCode.UNSPECIFIED_ERROR, ex.getCause().getMessage());
            channel.writeAndFlush(unSpecifiedError);
        }
    }
}
