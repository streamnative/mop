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

import static io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage.MAGIC;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.util.Signal;
import io.streamnative.pulsar.handlers.mqtt.MQTTCommonInboundHandler;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqttAdapterDecoder extends ReplayingDecoder<MqttAdapterDecoder.State> {

    public static final String NAME = "adapter-decoder";

    private final MqttDecoder mqttDecoder;
    private final Method decode;
    private final Header header = new Header();

    public MqttAdapterDecoder(int maxBytesInMessage) {
        super(State.MAGIC);
        this.mqttDecoder = new MqttDecoder(maxBytesInMessage);
        try {
            decode = mqttDecoder.getClass().getDeclaredMethod("decode",
                    ChannelHandlerContext.class, ByteBuf.class, List.class);
            decode.setAccessible(true);
        } catch (NoSuchMethodException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    static final Signal REPLAY = Signal.valueOf(ReplayingDecoder.class, "REPLAY");

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        switch (state()) {
            case MAGIC:
                if (!isAdapter(in.readByte())) {
                    in.resetReaderIndex();
                    List<MqttMessage> mqttMessages = new ArrayList<>();
                    try {
                        decode.invoke(mqttDecoder, ctx, in, mqttMessages);
                    } catch (InvocationTargetException ex) {
                        if (ex.getTargetException() instanceof Signal) {
                            ((Signal) ex.getTargetException()).expect(REPLAY);
                            throw (Signal) ex.getTargetException();
                        }
                    }
                    for (MqttMessage mqttMessage : mqttMessages) {
                        MqttAdapterMessage adapterMessage = new MqttAdapterMessage(MQTTCommonInboundHandler.NAME,
                                mqttMessage);
                        out.add(adapterMessage);
                    }
                    checkpoint(State.MAGIC);
                    return;
                } else {
                    checkpoint(State.VERSION);
                }
            case VERSION:
                header.setVersion(in.readByte());
                checkpoint(State.CLIENT_ID_LENGTH);
            case CLIENT_ID_LENGTH:
                header.setClientIdLength(in.readInt());
                checkpoint(State.CLIENT_ID);
            case CLIENT_ID:
                byte[] clientId = new byte[header.getClientIdLength()];
                in.readBytes(clientId);
                header.setClientId(new String(clientId, StandardCharsets.UTF_8));
                checkpoint(State.BODY);
            case BODY:
                List<Object> mqttMessages = new ArrayList<>();
                decode.invoke(mqttDecoder, ctx, in, mqttMessages);
                MqttMessage mqttMessage = (MqttMessage) mqttMessages.get(0);
                MqttAdapterMessage adapterMessage = new MqttAdapterMessage(header.getClientId(), mqttMessage);
                adapterMessage.setAdapter(true);
                out.add(adapterMessage);
                checkpoint(State.MAGIC);
        }
    }

    private boolean isAdapter(byte magic) {
        return magic == MAGIC;
    }

    enum State{
        MAGIC,
        VERSION,
        CLIENT_ID_LENGTH,
        CLIENT_ID,
        BODY;
    }

    @Getter
    @Setter
    class Header {
        int clientIdLength;
        String clientId;
        byte version;
    }
}
