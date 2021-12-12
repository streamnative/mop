package io.streamnative.pulsar.handlers.mqtt.exception.handler;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.streamnative.pulsar.handlers.mqtt.annotation.Ignore;
import io.streamnative.pulsar.handlers.mqtt.exception.*;
import io.streamnative.pulsar.handlers.mqtt.messages.codes.mqtt5.Mqtt5ConnReasonCode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractMqttExceptionHandler implements MqttExceptionHandler {

    @Override
    public void handleProtocolVersionNotSupport(Channel channel, MQTTProtocolVersionNotSupportException ex) {
        log.error(ex.getMessage());
        MqttConnAckMessage connAckMessage = MqttMessageBuilders
                .connAck()
                .returnCode(Mqtt5ConnReasonCode.UNSUPPORTED_PROTOCOL_VERSION.convertToNettyKlass())
                .build();
        channel.writeAndFlush(connAckMessage);
        channel.close();
    }

    @Override
    @Ignore
    public void handleConnQosNotSupport(Channel channel, MQTTQosNotSupportException ex) {
    }

    @Override
    @Ignore
    public void handleDisconnectionProtocolError(Channel channel, MQTTDisconnectProtocolErrorException ex) {
    }

    @Override
    @Ignore
    public void handlePubExceedServerMaximumReceive(Channel channel, MQTTExceedServerReceiveMaximumException ex) {
    }

    @Override
    @Ignore
    public void handlePubNoMatchingSubscriber(Channel channel, MQTTNoMatchingSubscriberException ex) {
    }

    @Override
    @Ignore
    public void handleUnSubNoSubscriptionExisted(Channel channel, MQTTNoSubscriptionExistedException exception) {
    }
}
