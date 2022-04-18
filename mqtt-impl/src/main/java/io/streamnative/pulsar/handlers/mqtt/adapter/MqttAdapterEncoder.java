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
package io.streamnative.pulsar.handlers.mqtt.adapter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.util.ReferenceCountUtil;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@ChannelHandler.Sharable
@Slf4j
public class MqttAdapterEncoder extends MessageToMessageEncoder<MqttAdapterMessage> {

    public static final String NAME = "adapter-encoder";

    private static final MqttEncoder ENCODER = MqttEncoder.INSTANCE;

    public static final MqttAdapterEncoder INSTANCE = new MqttAdapterEncoder();

    private final Method doEncode;

    public MqttAdapterEncoder() {
        try {
            doEncode = ENCODER.getClass().getDeclaredMethod("doEncode", ChannelHandlerContext.class,
                    MqttMessage.class);
            doEncode.setAccessible(true);
        } catch (NoSuchMethodException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, MqttAdapterMessage msg, List<Object> out) throws Exception {
        ByteBuf buffer;
        if (msg.isAdapter()) {
            ByteBuf mqtt = (ByteBuf) doEncode.invoke(ENCODER, ctx, msg.getMqttMessage());
            byte[] clientId = msg.getClientId().getBytes(StandardCharsets.UTF_8);
            buffer = ctx.alloc().buffer(1 + 1 + 4 + clientId.length + 4 + mqtt.readableBytes());
            buffer.writeByte(MqttAdapterMessage.MAGIC);
            buffer.writeByte(msg.getVersion());
            buffer.writeInt(clientId.length);
            buffer.writeBytes(clientId);
            buffer.writeInt(mqtt.readableBytes());
            buffer.writeBytes(mqtt);
            ReferenceCountUtil.safeRelease(mqtt);
        } else {
            buffer = (ByteBuf) doEncode.invoke(ENCODER, ctx, msg.getMqttMessage());
        }
        out.add(buffer);
    }
}
