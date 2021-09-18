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
package io.streamnative.pulsar.handlers.mqtt.psk;

import static org.apache.pulsar.client.impl.PulsarChannelInitializer.TLS_HANDLER;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslHandler;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKConfiguration;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKSecretKey;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * PSK client.
 */
@Slf4j
public class PSKClient extends ChannelInitializer<SocketChannel> {

    private String hint;

    private String identity;

    private String pwd;

    public PSKClient(String hint, String identity, String pwd) {
        this.hint = hint;
        this.identity = identity;
        this.pwd = pwd;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        PSKConfiguration pskConfiguration = new PSKConfiguration();
        PSKSecretKey pskSecretKey = new PSKSecretKey(identity, pwd);
        pskSecretKey.setHint(hint);
        pskConfiguration.setSecretKey(pskSecretKey);
        ch.pipeline().addLast(TLS_HANDLER, new SslHandler(PSKUtils.createClientEngine(ch, pskConfiguration)));
        ch.pipeline().addLast("decoder", new MqttDecoder());
        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
        ch.pipeline().addLast("handler", new PSKInboundHandler());
    }

    class PSKInboundHandler extends ChannelInboundHandlerAdapter{

        public void channelActive(ChannelHandlerContext ctx) {
            log.info("channelActive id : {}", ctx.channel().id());
        }
    }
}
