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
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.streamnative.pulsar.handlers.mqtt.annotation.Ignore;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTDisconnectProtocolErrorException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTExceedServerReceiveMaximumException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTProtocolVersionNotSupportException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTQosNotSupportException;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import lombok.extern.slf4j.Slf4j;

/**
 * Exception handler.
 * This handler abstract all same behavior to handle exception in mqtt server.
 * @see MqttV3xExceptionHandler
 * @see MqttV5ExceptionHandler
 */
@Slf4j
public abstract class AbstractMqttExceptionHandler implements MqttExceptionHandler {

    /**
     * When protocol version is not support at connect method, we should return unsupported reason code at connect ack.
     * @param channel  Channel
     * @param ex  MQTTProtocolVersionNotSupportException
     */
    @Override
    public void handleConnProtocolVersionNotSupport(Channel channel, MQTTProtocolVersionNotSupportException ex) {
        log.error(ex.getMessage());
        MqttConnAckMessage connAckMessage = MqttMessageBuilders
                .connAck()
                .returnCode(Mqtt5ConnReasonCode.UNSUPPORTED_PROTOCOL_VERSION.convertToNettyKlass())
                .build();
        channel.writeAndFlush(connAckMessage);
        channel.close();
    }

    /**
     * The default behavior to ignore this exception.
     * @param channel Channel
     * @param ex MQTTQosNotSupportException
     */
    @Override
    @Ignore
    public void handleConnQosNotSupport(Channel channel, MQTTQosNotSupportException ex) {
    }
    /**
     * The default behavior to ignore this exception.
     * @param channel Channel
     * @param ex MQTTDisconnectProtocolErrorException
     */
    @Override
    @Ignore
    public void handleDisconnectionProtocolError(Channel channel, MQTTDisconnectProtocolErrorException ex) {
    }
    /**
     * The default behavior to ignore this exception.
     * @param channel Channel
     * @param ex MQTTExceedServerReceiveMaximumException
     */
    @Override
    @Ignore
    public void handlePubExceedServerMaximumReceive(Channel channel, MQTTExceedServerReceiveMaximumException ex) {
    }
    /**
     * The default behavior to ignore this exception.
     * @param channel Channel
     * @param ex MQTTNoMatchingSubscriberException
     */
    @Override
    @Ignore
    public void handlePubNoMatchingSubscriber(Channel channel, MQTTNoMatchingSubscriberException ex) {
    }
    /**
     * The default behavior to ignore this exception.
     * @param channel Channel
     * @param ex MQTTNoSubscriptionExistedException
     */
    @Override
    @Ignore
    public void handleUnSubNoSubscriptionExisted(Channel channel, MQTTNoSubscriptionExistedException ex) {
    }
}
