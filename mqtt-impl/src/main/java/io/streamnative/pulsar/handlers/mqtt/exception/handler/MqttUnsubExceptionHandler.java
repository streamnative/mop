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
package io.streamnative.pulsar.handlers.mqtt.exception.handler;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTTopicNotExistedException;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5UnsubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttUnsubAckMessageHelper;


public class MqttUnsubExceptionHandler implements MqttExceptionHandler {
    @Override
    public void handleVersion3(int identifier, Channel channel, Throwable ex) {
        channel.close();
    }

    @Override
    public void handleVersion5(int identifier, Channel channel, Throwable ex) {
        MqttMessage ackMessage;
        if (ex.getCause() instanceof MQTTNoSubscriptionExistedException) {
            ackMessage = MqttUnsubAckMessageHelper.createMqtt5(identifier,
                    Mqtt5UnsubReasonCode.NO_SUBSCRIPTION_EXISTED, ex.getCause().getMessage());
        } else if (ex.getCause() instanceof MQTTServerException) {
            ackMessage =
                    MqttUnsubAckMessageHelper.createMqtt5(identifier,
                            Mqtt5UnsubReasonCode.IMPLEMENTATION_SPECIFIC_ERROR, ex.getCause().getMessage());
        } else if (ex.getCause() instanceof MQTTTopicNotExistedException) {
            ackMessage =
                    MqttUnsubAckMessageHelper.createMqtt5(identifier,
                            Mqtt5UnsubReasonCode.TOPIC_FILTER_INVALID, ex.getCause().getMessage());
        } else {
            ackMessage = MqttUnsubAckMessageHelper.createMqtt5(identifier,
                    Mqtt5UnsubReasonCode.UNSPECIFIED_ERROR, ex.getCause().getMessage());
        }
        channel.writeAndFlush(ackMessage);
    }
}
