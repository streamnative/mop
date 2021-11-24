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
package io.streamnative.pulsar.handlers.mqtt.utils;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTTopicNotExistedException;
import io.streamnative.pulsar.handlers.mqtt.messages.MQTTPubAckMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.MQTTSubAckMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.MQTTUnsubAckMessageUtils;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttPubAckReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttSubAckReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.MqttUnsubAckReasonCode;
import org.apache.pulsar.broker.service.BrokerServiceException;

/**
 * MQTT version 5.0 exception handler.
 * use this handler to implement reason code.
 */
public class MQTT5ExceptionUtils {

    /**
     * handle ubSubscribe Exception.
     *
     * @param messageID - Mqtt message exception
     * @param channel   - Netty Nio channel
     * @param ex        - exception
     */
    public static void handleUnSubscribeException(int messageID, Channel channel, Throwable ex) {
        if (ex.getCause() instanceof MQTTNoSubscriptionExistedException) {
            MqttMessage unSubscriptionExistedAckMessage = MQTTUnsubAckMessageUtils.createMqtt5(messageID,
                    MqttUnsubAckReasonCode.NO_SUBSCRIPTION_EXISTED, ex.getCause().getMessage());
            channel.writeAndFlush(unSubscriptionExistedAckMessage);
        } else if (ex.getCause() instanceof MQTTServerException) {
            MqttMessage implementationSpecificErrorAckMessage =
                    MQTTUnsubAckMessageUtils.createMqtt5(messageID,
                            MqttUnsubAckReasonCode.IMPLEMENTATION_SPECIFIC_ERROR, ex.getCause().getMessage());
            channel.writeAndFlush(implementationSpecificErrorAckMessage);
        } else if (ex.getCause() instanceof MQTTTopicNotExistedException) {
            MqttMessage topicFilterInvalidAckMessage =
                    MQTTUnsubAckMessageUtils.createMqtt5(messageID,
                            MqttUnsubAckReasonCode.TOPIC_FILTER_INVALID, ex.getCause().getMessage());
            channel.writeAndFlush(topicFilterInvalidAckMessage);
        } else {
            MqttMessage unSpecifiedError = MQTTUnsubAckMessageUtils.createMqtt5(messageID,
                    MqttUnsubAckReasonCode.UNSPECIFIED_ERROR, ex.getCause().getMessage());
            channel.writeAndFlush(unSpecifiedError);
        }
    }

    /**
     * handle subscribe Exception.
     *
     * @param messageID - Mqtt message exception
     * @param channel   - Netty Nio channel
     * @param ex        - exception
     */
    public static void handleSubScribeException(int messageID, Channel channel, Throwable ex) {
        if (ex.getCause() instanceof MQTTServerException) {
            MqttMessage mqtt5SubAckMessage =
                    MQTTSubAckMessageUtils.createMqtt5(messageID, MqttSubAckReasonCode.IMPLEMENTATION_SPECIFIC_ERROR,
                            ex.getCause().getMessage());
            channel.writeAndFlush(mqtt5SubAckMessage);
        } else {
            MqttMessage subscribeAckMessage = MQTTSubAckMessageUtils
                    .createMqtt5(messageID, MqttSubAckReasonCode.UNSPECIFIED_ERROR, ex.getCause().getMessage());
            channel.writeAndFlush(subscribeAckMessage);
        }
    }

    /**
     * handle publish Exception.
     *
     * @param packetId - Mqtt packet exception
     * @param channel  - Netty Nio channel
     * @param ex       - exception
     */
    public static void handlePublishException(int packetId, Channel channel, Throwable ex) {
        MqttMessage unspecifiedErrorPubAckMessage;
        if (ex instanceof BrokerServiceException.TopicNotFoundException) {
             unspecifiedErrorPubAckMessage =
                    MQTTPubAckMessageUtils.createMqtt5(packetId, MqttPubAckReasonCode.TOPIC_NAME_INVALID,
                            ex.getMessage());
        } else if (ex instanceof MQTTNoMatchingSubscriberException) {
            unspecifiedErrorPubAckMessage =
                    MQTTPubAckMessageUtils.createMqtt5(packetId, MqttPubAckReasonCode.NO_MATCHING_SUBSCRIBERS,
                            ex.getMessage());
        } else {
            unspecifiedErrorPubAckMessage =
                    MQTTPubAckMessageUtils.createMqtt5(packetId, MqttPubAckReasonCode.UNSPECIFIED_ERROR,
                            ex.getMessage());
        }
        channel.writeAndFlush(unspecifiedErrorPubAckMessage);
    }
}
