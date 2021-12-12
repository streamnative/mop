package io.streamnative.pulsar.handlers.mqtt.exception.handler;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GlobalExceptionHandler {
    private final boolean handled;
    private final Channel channel;
    private final Throwable ex;


    private GlobalExceptionHandler(boolean handled, Channel channel, Throwable ex) {
        this.handled = handled;
        this.channel = channel;
        this.ex = ex;
    }

    public static GlobalExceptionHandler handleServerException(Channel channel, Throwable ex) {
        boolean isHandled = false;
        if (ex instanceof MQTTServerException) {
            isHandled = MqttExceptionHelper.handleMqttServerException(channel, (MQTTServerException) ex);
        }
        return new GlobalExceptionHandler(isHandled, channel, ex);
    }

    public void orElseHandleCommon(MqttMessageType condition, int packetId) {
        if (handled) {
            return;
        }
        MqttExceptionHelper.handleCommonException(condition, channel, packetId, ex);
    }

}
