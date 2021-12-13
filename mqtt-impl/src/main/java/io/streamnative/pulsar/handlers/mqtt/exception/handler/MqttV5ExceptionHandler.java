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
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageIdAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.streamnative.pulsar.handlers.mqtt.annotation.NoNeedCloseChannel;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTBadUserNameOrPasswordException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTClientIdentifierNotValidException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTDisconnectProtocolErrorException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTExceedServerReceiveMaximumException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoMatchingSubscriberException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNotAuthorizedException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTQosNotSupportException;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerUnavailableException;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5PubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5SubReasonCode;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5UnsubReasonCode;
import lombok.extern.slf4j.Slf4j;

/**
 * The exception handler implements mqtt specification 5.x.
 * We need to return reason code at all ack method.
 * @see AbstractMqttExceptionHandler
 */
@Slf4j
public class MqttV5ExceptionHandler extends AbstractMqttExceptionHandler {
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
                .returnCode(Mqtt5ConnReasonCode.CLIENT_IDENTIFIER_NOT_VALID.convertToNettyKlass())
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
                .returnCode(Mqtt5ConnReasonCode.BAD_USERNAME_OR_PASSWORD.convertToNettyKlass())
                .build();
        channel.writeAndFlush(ackMessage);
        channel.close();
    }

    /**
     * When qos not support at connect method.
     * @param channel Channel
     * @param ex MQTTQosNotSupportException
     */
    @Override
    public void handleConnQosNotSupport(Channel channel, MQTTQosNotSupportException ex) {
        log.error(ex.getMessage());
        MqttConnAckMessage ackMessage = MqttMessageBuilders
                .connAck()
                .returnCode(Mqtt5ConnReasonCode.QOS_NOT_SUPPORTED.convertToNettyKlass())
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
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        ex.getMessage());
        mqttProperties.add(reasonStringProperty);
        MqttConnAckMessage ackMessage = MqttMessageBuilders
                .connAck()
                .returnCode(Mqtt5ConnReasonCode.SERVER_UNAVAILABLE.convertToNettyKlass())
                .properties(mqttProperties)
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
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        ex.getMessage());
        mqttProperties.add(reasonStringProperty);
        MqttMessage ackMessage = MqttMessageBuilders
                .pubAck()
                .packetId(ex.getPacketId())
                .reasonCode(Mqtt5PubReasonCode.NOT_AUTHORIZED.byteValue())
                .properties(mqttProperties)
                .build();
        channel.writeAndFlush(ackMessage);
        channel.close();
    }

    /**
     * When client send message exceed server maximum receive at publish method.
     * @param channel Channel
     * @param ex MQTTExceedServerReceiveMaximumException
     */
    @Override
    public void handlePubExceedServerMaximumReceive(Channel channel, MQTTExceedServerReceiveMaximumException ex) {
        log.error(ex.getMessage());
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        ex.getMessage());
        mqttProperties.add(reasonStringProperty);
        MqttMessage ackMessage = MqttMessageBuilders
                .pubAck()
                .packetId(ex.getPacketId())
                .reasonCode(Mqtt5ConnReasonCode.QUOTA_EXCEEDED.byteValue())
                .properties(mqttProperties)
                .build();
        channel.writeAndFlush(ackMessage);
        channel.close();
    }

    /**
     * When client send wrong property value at disconnect method.
     * @param channel Channel
     * @param ex MQTTDisconnectProtocolErrorException
     */
    @Override
    public void handleDisconnectionProtocolError(Channel channel, MQTTDisconnectProtocolErrorException ex) {
        log.error(ex.getMessage());
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        ex.getMessage());
        mqttProperties.add(reasonStringProperty);
        MqttMessage ackMessage = MqttMessageBuilders
                .disconnect()
                .reasonCode(Mqtt5ConnReasonCode.PROTOCOL_ERROR.byteValue())
                .properties(mqttProperties)
                .build();
        channel.writeAndFlush(ackMessage);
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
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        ex.getMessage());
        mqttProperties.add(reasonStringProperty);
        MqttSubAckMessage subAckMessage = getMqttSubFailAckMessage(ex.getPacketId(), mqttProperties,
                Mqtt5SubReasonCode.NOT_AUTHORIZED);
        channel.writeAndFlush(subAckMessage);
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
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        ex.getMessage());
        mqttProperties.add(reasonStringProperty);
        MqttSubAckMessage subAckMessage = getMqttSubFailAckMessage(packetId, mqttProperties,
                Mqtt5SubReasonCode.UNSPECIFIED_ERROR);
        channel.writeAndFlush(subAckMessage);
        channel.close();
    }

    private MqttSubAckMessage getMqttSubFailAckMessage(int packetId, MqttProperties mqttProperties,
                                                       Mqtt5SubReasonCode unspecifiedError) {
        MqttFixedHeader mqttFixedHeader =
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessageIdAndPropertiesVariableHeader mqttSubAckVariableHeader =
                new MqttMessageIdAndPropertiesVariableHeader(packetId, mqttProperties);
        MqttSubAckPayload subAckPayload = new MqttSubAckPayload(unspecifiedError.value());
        return new MqttSubAckMessage(mqttFixedHeader, mqttSubAckVariableHeader,
                subAckPayload);
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
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        ex.getMessage());
        mqttProperties.add(reasonStringProperty);
        MqttMessage pubAck = MqttMessageBuilders.pubAck()
                .packetId(packetId)
                .reasonCode(Mqtt5PubReasonCode.UNSPECIFIED_ERROR.byteValue())
                .build();
        channel.writeAndFlush(pubAck);
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
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        ex.getMessage());
        mqttProperties.add(reasonStringProperty);
        MqttUnsubAckMessage ackMessage = MqttMessageBuilders.unsubAck()
                .packetId(packetId)
                .properties(mqttProperties)
                .addReasonCode(Mqtt5UnsubReasonCode.UNSPECIFIED_ERROR.shortValue())
                .build();
        channel.writeAndFlush(ackMessage);
        channel.close();
    }

    /**
     * When no matching subscriber at publish method.
     * @param channel Channel
     * @param ex MQTTNoMatchingSubscriberException
     */
    @Override
    @NoNeedCloseChannel
    public void handlePubNoMatchingSubscriber(Channel channel, MQTTNoMatchingSubscriberException ex) {
        log.warn(ex.getMessage());
        MqttMessage ackMsg = MqttMessageBuilders
                .pubAck()
                .packetId(ex.getPacketId())
                .reasonCode(Mqtt5PubReasonCode.NO_MATCHING_SUBSCRIBERS.byteValue())
                .build();
        channel.writeAndFlush(ackMsg);
    }
    /**
     * When no subscriber exist at unsubscibe method.
     * @param channel Channel
     * @param ex MQTTNoMatchingSubscriberException
     */
    @Override
    @NoNeedCloseChannel
    public void handleUnSubNoSubscriptionExisted(Channel channel, MQTTNoSubscriptionExistedException ex) {
        log.error(ex.getMessage());
        MqttProperties mqttProperties = new MqttProperties();
        MqttProperties.StringProperty reasonStringProperty =
                new MqttProperties.StringProperty(MqttProperties.MqttPropertyType.REASON_STRING.value(),
                        ex.getMessage());
        mqttProperties.add(reasonStringProperty);
        MqttUnsubAckMessage ackMessage = MqttMessageBuilders
                .unsubAck()
                .packetId(ex.getPacketId())
                .properties(mqttProperties)
                .addReasonCode(Mqtt5UnsubReasonCode.NO_SUBSCRIPTION_EXISTED.shortValue())
                .build();
        channel.writeAndFlush(ackMessage);
    }
}
