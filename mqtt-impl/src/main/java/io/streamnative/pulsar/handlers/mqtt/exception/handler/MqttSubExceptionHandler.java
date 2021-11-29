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
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.factory.MqttSubAckMessageHelper;

public class MqttSubExceptionHandler implements MqttExceptionHandler {
    @Override
    public void handleVersion3(int identifier, Channel channel, Throwable ex) {
        MqttSubAckMessage ackMessage = MqttSubAckMessageHelper
                .createMqtt(identifier, Mqtt3SubReasonCode.FAILURE);
        channel.writeAndFlush(ackMessage);
        channel.close();
    }

    @Override
    public void handleVersion5(int identifier, Channel channel, Throwable ex) {
        MqttMessage ackMessage;
        if (ex.getCause() instanceof MQTTServerException) {
            ackMessage =
                    MqttSubAckMessageHelper.createMqtt5(identifier, Mqtt5SubReasonCode.IMPLEMENTATION_SPECIFIC_ERROR,
                            ex.getCause().getMessage());
        } else {
            ackMessage = MqttSubAckMessageHelper
                    .createMqtt5(identifier, Mqtt5SubReasonCode.UNSPECIFIED_ERROR, ex.getCause().getMessage());
        }
        channel.writeAndFlush(ackMessage);
    }
}
