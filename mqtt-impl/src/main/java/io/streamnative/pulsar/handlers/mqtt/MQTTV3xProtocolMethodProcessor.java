package io.streamnative.pulsar.handlers.mqtt;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.messages.MqttPropertyUtils;

public class MQTTV3xProtocolMethodProcessor extends AbstractProtocolMethodProcessor {

    public MQTTV3xProtocolMethodProcessor(MQTTService mqttService, ChannelHandlerContext ctx) {
        super(mqttService, ctx);
    }


    @Override
    Connection buildConnection(Connection.ConnectionBuilder connectionBuilder, MqttConnectMessage msg) {
        connectionBuilder
                .clientReceiveMaximum(MqttPropertyUtils.BEFORE_DEFAULT_RECEIVE_MAXIMUM);
        return connectionBuilder.build();
    }

    @Override
    void checkWillingMessageIfNeeded(String clientId, int willQos) {
        // don't check willing message
    }

    @Override
    void checkServerReceivePubMessageAndIncrementCounterIfNeeded(Channel channel, MqttPublishMessage msg) {
        // don't check any things
    }

    @Override
    void parseDisconnectPropertiesIfNeeded(MqttMessage msg) {
        // don't parse
    }
}
