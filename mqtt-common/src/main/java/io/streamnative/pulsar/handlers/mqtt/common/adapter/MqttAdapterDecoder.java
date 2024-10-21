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
package io.streamnative.pulsar.handlers.mqtt.common.adapter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MqttAdapterDecoder extends ReplayingDecoder<MqttAdapterDecoder.State> {

    public static final String NAME = "adapter-decoder";

    private final Header header = new Header();

    public MqttAdapterDecoder() {
        super(State.MAGIC);
    }

    @SuppressWarnings("checkstyle:FallThrough")
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state()) {
            case MAGIC:
                in.markReaderIndex();
                if (!isAdapter(in.readByte())) {
                    in.resetReaderIndex();
                    ctx.pipeline().addAfter("mqtt-decoder", CombineHandler.NAME, new CombineHandler());
                    if (in.isReadable()) {
                        out.add(in.readBytes(super.actualReadableBytes()));
                    }
                    ctx.pipeline().remove(CombineAdapterHandler.NAME);
                    ctx.pipeline().remove(this);
                    break;
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
                checkpoint(State.BODY_LENGTH);
            case BODY_LENGTH:
                header.setBodyLength(in.readInt());
                checkpoint(State.BODY);
            case BODY:
                ByteBuf mqttBuf = in.readBytes(header.getBodyLength());
                MqttAdapterMessage adapterMsg = new MqttAdapterMessage(header.version, header.clientId);
                out.add(adapterMsg);
                out.add(mqttBuf);
                checkpoint(State.MAGIC);
        }
    }

    private boolean isAdapter(byte magic) {
        return magic == MqttAdapterMessage.MAGIC;
    }

    enum State{
        MAGIC,
        VERSION,
        CLIENT_ID_LENGTH,
        CLIENT_ID,
        BODY_LENGTH,
        BODY;
    }

    @Getter
    @Setter
    static class Header {
        byte version;
        int clientIdLength;
        String clientId;
        int bodyLength;
    }
}
