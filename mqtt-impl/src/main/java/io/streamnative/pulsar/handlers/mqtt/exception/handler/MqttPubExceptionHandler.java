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
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttPubAckMessageHelper;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.zookeeper.KeeperException;

public class MqttPubExceptionHandler implements MqttExceptionHandler {
    @Override
    public void handleVersion3(int identifier, Channel channel, Throwable ex) {
        closeIfKeeperException(channel, ex);
    }

    @Override
    public void handleVersion5(int identifier, Channel channel, Throwable ex) {
        MqttMessage unspecifiedErrorPubAckMessage;
        if (ex instanceof BrokerServiceException.TopicNotFoundException) {
            unspecifiedErrorPubAckMessage =
                    MqttPubAckMessageHelper.createMqtt5(identifier, Mqtt5PubReasonCode.TOPIC_NAME_INVALID,
                            ex.getMessage());
        } else if (ex instanceof MQTTNoMatchingSubscriberException) {
            unspecifiedErrorPubAckMessage =
                    MqttPubAckMessageHelper.createMqtt5(identifier, Mqtt5PubReasonCode.NO_MATCHING_SUBSCRIBERS,
                            ex.getMessage());
        } else {
            unspecifiedErrorPubAckMessage =
                    MqttPubAckMessageHelper.createMqtt5(identifier, Mqtt5PubReasonCode.UNSPECIFIED_ERROR,
                            ex.getMessage());
        }
        channel.writeAndFlush(unspecifiedErrorPubAckMessage);
        closeIfKeeperException(channel, ex);
    }

    private void closeIfKeeperException(Channel channel, Throwable ex) {
        if (ex instanceof BrokerServiceException.ServerMetadataException) {
            if (ex.getCause() instanceof KeeperException.NoNodeException) {
                channel.close();
            }
        }
    }
}
