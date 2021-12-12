package io.streamnative.pulsar.handlers.mqtt;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import io.streamnative.pulsar.handlers.mqtt.utils.NettyUtils;

public class DelegateProtocolMethodProcessor extends AbstractProtocolMethodProcessor {
    private final MQTTV3xProtocolMethodProcessor v3Delegate;
    private final MQTTV5ProtocolMethodProcessor v5Delegate;

    public DelegateProtocolMethodProcessor(MQTTService mqttService, ChannelHandlerContext ctx) {
        super(mqttService, ctx);
        this.v3Delegate = new MQTTV3xProtocolMethodProcessor(mqttService, ctx);
        this.v5Delegate = new MQTTV5ProtocolMethodProcessor(mqttService, ctx);
    }

    @Override
    void checkWillingMessageIfNeeded(String clientId, int willQos) {
        int protocolVersion = NettyUtils.getProtocolVersion(serverCnx.ctx().channel());
        switch (protocolVersion) {
            case 3:
            case 4:
                v3Delegate.checkWillingMessageIfNeeded(clientId, willQos);
                break;
            case 5:
                v5Delegate.checkWillingMessageIfNeeded(clientId, willQos);
                break;
        }
    }

    @Override
    Connection buildConnection(Connection.ConnectionBuilder connectionBuilder, MqttConnectMessage msg) {
        int protocolVersion = NettyUtils.getProtocolVersion(serverCnx.ctx().channel());
        switch (protocolVersion) {
            case 3:
            case 4:
                return v3Delegate.buildConnection(connectionBuilder, msg);
            case 5:
                return v5Delegate.buildConnection(connectionBuilder, msg);
            default:
                throw new MQTTServerException("unsupported protocol version");
        }
    }

    @Override
    void checkServerReceivePubMessageAndIncrementCounterIfNeeded(Channel channel, MqttPublishMessage msg) {
        int protocolVersion = NettyUtils.getProtocolVersion(serverCnx.ctx().channel());
        switch (protocolVersion) {
            case 3:
            case 4:
                v3Delegate.checkServerReceivePubMessageAndIncrementCounterIfNeeded(channel, msg);
                break;
            case 5:
                v5Delegate.checkServerReceivePubMessageAndIncrementCounterIfNeeded(channel, msg);
                break;
        }
    }

    @Override
    void parseDisconnectPropertiesIfNeeded(MqttMessage msg) {
        int protocolVersion = NettyUtils.getProtocolVersion(serverCnx.ctx().channel());
        switch (protocolVersion) {
            case 3:
            case 4:
                v3Delegate.parseDisconnectPropertiesIfNeeded(msg);
                break;
            case 5:
                v5Delegate.parseDisconnectPropertiesIfNeeded(msg);
                break;
        }
    }
}
