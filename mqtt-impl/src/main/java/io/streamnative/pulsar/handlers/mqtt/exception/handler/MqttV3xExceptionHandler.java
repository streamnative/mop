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
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTBadUserNameOrPasswordException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTClientIdentifierNotValidException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNotAuthorizedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerUnavailableException;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt3.Mqtt3SubReasonCode;
import lombok.extern.slf4j.Slf4j;

/**
 * The exception handler implements mqtt specification 3.x.
 * @see AbstractMqttExceptionHandler
 */
@Slf4j
public class MqttV3xExceptionHandler extends AbstractMqttExceptionHandler {
    /**
     * When client id is not valid at connect method.
     * @param channel Netty channel
     * @param ex MQTTClientIdentifierNotValidException
     */
    @Override
    public void handleConnClientIdentifierNotValid(Channel channel, MQTTClientIdentifierNotValidException ex) {
        log.error(ex.getMessage());
        MqttConnAckMessage ackMessage = MqttMessageBuilders
                .connAck()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED.convertToNettyKlass())
                .build();
        channel.writeAndFlush(ackMessage);
        channel.close();
    }

    /**
     * When authentication fail by bad name or password at connect method.
     * @param channel Netty channel
     * @param ex MQTTBadUserNameOrPasswordException
     */
    @Override
    public void handleConnBadUserNameOrPassword(Channel channel, MQTTBadUserNameOrPasswordException ex) {
        log.error(ex.getMessage());
        MqttConnAckMessage ackMessage = MqttMessageBuilders
                .connAck()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD.convertToNettyKlass())
                .build();
        channel.writeAndFlush(ackMessage);
        channel.close();
    }

    /**
     * When server unavailable at connect method.
     * @param channel Netty channel
     * @param ex MQTTServerUnavailableException
     */
    @Override
    public void handleConnServerUnavailable(Channel channel, MQTTServerUnavailableException ex) {
        log.error(ex.getMessage());
        MqttConnAckMessage ackMessage = MqttMessageBuilders
                .connAck()
                .returnCode(Mqtt3ConnReasonCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE.convertToNettyKlass())
                .build();
        channel.writeAndFlush(ackMessage);
        channel.close();
    }

    /**
     * When authorization fail at publish method.
     * @param channel Netty channel
     * @param ex MQTTNotAuthorizedException
     */
    @Override
    public void handlePubNotAuthorized(Channel channel, MQTTNotAuthorizedException ex) {
        log.error(ex.getMessage());
        channel.close();
    }

    /**
     * When authorization fail at subscribe method.
     * @param channel Netty channel
     * @param ex MQTTNotAuthorizedException
     */
    @Override
    public void handleSubNotAuthorized(Channel channel, MQTTNotAuthorizedException ex) {
        log.error(ex.getMessage());
        MqttSubAckMessage subAckMessage = MqttMessageBuilders
                .subAck()
                .build();
        subAckMessage.payload().reasonCodes().add(Mqtt3SubReasonCode.FAILURE.value());
        channel.writeAndFlush(subAckMessage);
        channel.close();
    }

    /**
     * Common exception at unsubscribe method.
     * @param channel Netty channel
     * @param packetId packet id
     * @param ex Throwable
     */
    @Override
    public void handleUnSubCommonException(Channel channel, int packetId, Throwable ex) {
        log.error(ex.getMessage());
        channel.close();
    }
    /**
     * Common exception at subscribe common method.
     * @param channel Netty channel
     * @param packetId packet id
     * @param ex Throwable
     */
    @Override
    public void handleSubCommonException(Channel channel, int packetId, Throwable ex) {
        log.error(ex.getMessage());
        channel.close();
    }
    /**
     * Common exception at publish method.
     * @param channel Netty channel
     * @param packetId packet id
     * @param ex Throwable
     */
    @Override
    public void handlePubCommonException(Channel channel, int packetId, Throwable ex) {
        log.error(ex.getMessage());
        channel.close();
    }

}
