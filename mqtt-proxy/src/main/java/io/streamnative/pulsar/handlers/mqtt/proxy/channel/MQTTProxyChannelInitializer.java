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
package io.streamnative.pulsar.handlers.mqtt.proxy.channel;

import static org.apache.pulsar.client.impl.PulsarChannelInitializer.TLS_HANDLER;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.CombineAdapterHandler;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterDecoder;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterEncoder;
import io.streamnative.pulsar.handlers.mqtt.common.psk.PSKUtils;
import io.streamnative.pulsar.handlers.mqtt.common.tls.MQTTTlsFactory;
import io.streamnative.pulsar.handlers.mqtt.common.utils.WebSocketUtils;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyConfiguration;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import io.streamnative.pulsar.handlers.mqtt.proxy.impl.MQTTProxyException;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * Proxy service channel initializer.
 */
public class MQTTProxyChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final MQTTProxyService proxyService;
    @Getter
    private final MQTTProxyConfiguration proxyConfig;

    private final boolean enableTls;
    private final boolean enableTlsPsk;
    private final boolean enableWs;
    private MQTTTlsFactory tlsFactory;

    public MQTTProxyChannelInitializer(MQTTProxyService proxyService, MQTTProxyConfiguration proxyConfig,
                                       boolean enableTls, boolean enableWs,
                                       ScheduledExecutorService sslContextRefresher) throws MQTTProxyException {
        this(proxyService, proxyConfig, enableTls, false, enableWs, sslContextRefresher);
    }

    public MQTTProxyChannelInitializer(MQTTProxyService proxyService, MQTTProxyConfiguration proxyConfig,
                                       boolean enableTls, boolean enableTlsPsk, boolean enableWs,
                                       ScheduledExecutorService sslContextRefresher) throws MQTTProxyException {
        try {
            this.proxyService = proxyService;
            this.proxyConfig = proxyConfig;
            this.enableTls = enableTls;
            this.enableTlsPsk = enableTlsPsk;
            this.enableWs = enableWs;
            if (this.enableTls) {
                this.tlsFactory = new MQTTTlsFactory(proxyConfig, TlsPurpose.PROXY, sslContextRefresher);
            }
        } catch (Exception e) {
            throw new MQTTProxyException(e);
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(30, 0, 0));
        if (this.enableTls) {
            ch.pipeline().addLast(TLS_HANDLER, tlsFactory.newServerSslHandler(ch.alloc()));
        } else if (this.enableTlsPsk) {
            ch.pipeline().addLast(TLS_HANDLER,
                    new SslHandler(PSKUtils.createServerEngine(ch, proxyService.getPskConfiguration())));
        }
        if (this.enableWs) {
            WebSocketUtils.addWsHandler(ch.pipeline(), proxyConfig);
        }
        ch.pipeline().addLast(MqttAdapterDecoder.NAME, new MqttAdapterDecoder());
        ch.pipeline().addLast("mqtt-decoder", new MqttDecoder(proxyConfig.getMqttMessageMaxLength()));
        // Encoder
        ch.pipeline().addLast(MqttAdapterEncoder.NAME, MqttAdapterEncoder.INSTANCE);
        // Handler
        ch.pipeline().addLast(CombineAdapterHandler.NAME, new CombineAdapterHandler());
        ch.pipeline().addLast("handler", new MQTTProxyInboundHandler(proxyService));
    }

}
