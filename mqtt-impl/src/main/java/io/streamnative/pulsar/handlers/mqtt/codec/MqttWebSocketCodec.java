package io.streamnative.pulsar.handlers.mqtt.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.util.List;

/**
 * WebSocket Mqtt message codec.
 */
public class MqttWebSocketCodec extends MessageToMessageCodec<BinaryWebSocketFrame, ByteBuf> {

    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        out.add(new BinaryWebSocketFrame(msg.retain()));
    }

    protected void decode(ChannelHandlerContext ctx, BinaryWebSocketFrame msg, List<Object> out) throws Exception {
        out.add(msg.retain().content());
    }
}
