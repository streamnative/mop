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

@Slf4j
public class MqttV3xExceptionHandler extends AbstractMqttExceptionHandler {
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

    @Override
    public void handlePubNotAuthorized(Channel channel, MQTTNotAuthorizedException ex) {
        log.error(ex.getMessage());
        channel.close();
    }

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

    @Override
    public void handleUnSubCommonException(Channel channel, int packetId, Throwable ex) {
        log.error(ex.getMessage());
        channel.close();
    }

    @Override
    public void handleSubCommonException(Channel channel, int packetId, Throwable ex) {
        log.error(ex.getMessage());
        channel.close();
    }

    @Override
    public void handlePubCommonException(Channel channel, int packetId, Throwable ex) {
        log.error(ex.getMessage());
        channel.close();
    }

}
