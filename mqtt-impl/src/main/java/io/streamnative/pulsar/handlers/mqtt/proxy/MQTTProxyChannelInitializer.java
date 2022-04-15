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
package io.streamnative.pulsar.handlers.mqtt.proxy;

import static org.apache.pulsar.client.impl.PulsarChannelInitializer.TLS_HANDLER;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterDecoder;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterEncoder;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKUtils;
import lombok.Getter;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;

/**
 * Proxy service channel initializer.
 */
public class MQTTProxyChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final MQTTProxyService proxyService;
    @Getter
    private final MQTTProxyConfiguration proxyConfig;

    private final boolean enableTls;
    private final boolean enableTlsPsk;
    private final boolean tlsEnabledWithKeyStore;

    private SslContextAutoRefreshBuilder<SslContext> serverSslCtxRefresher;
    private NettySSLContextAutoRefreshBuilder serverSSLContextAutoRefreshBuilder;

    public MQTTProxyChannelInitializer(MQTTProxyService proxyService, MQTTProxyConfiguration proxyConfig,
                                       boolean enableTls) {
        this(proxyService, proxyConfig, enableTls, false);
    }

    public MQTTProxyChannelInitializer(MQTTProxyService proxyService, MQTTProxyConfiguration proxyConfig,
                                       boolean enableTls, boolean enableTlsPsk) {
        this.proxyService = proxyService;
        this.proxyConfig = proxyConfig;
        this.enableTls = enableTls;
        this.enableTlsPsk = enableTlsPsk;
        this.tlsEnabledWithKeyStore = proxyConfig.isMqttTlsEnabledWithKeyStore();
        if (this.enableTls) {
            if (tlsEnabledWithKeyStore) {
                serverSSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                        proxyConfig.getMqttTlsProvider(),
                        proxyConfig.getMqttTlsKeyStoreType(),
                        proxyConfig.getMqttTlsKeyStore(),
                        proxyConfig.getMqttTlsKeyStorePassword(),
                        proxyConfig.isMqttTlsAllowInsecureConnection(),
                        proxyConfig.getMqttTlsTrustStoreType(),
                        proxyConfig.getMqttTlsTrustStore(),
                        proxyConfig.getMqttTlsTrustStorePassword(),
                        proxyConfig.isMqttTlsRequireTrustedClientCertOnConnect(),
                        proxyConfig.getMqttTlsCiphers(),
                        proxyConfig.getMqttTlsProtocols(),
                        proxyConfig.getMqttTlsCertRefreshCheckDurationSec());
            } else {
                serverSslCtxRefresher = new NettyServerSslContextBuilder(
                        proxyConfig.isMqttTlsAllowInsecureConnection(),
                        proxyConfig.getMqttTlsTrustCertsFilePath(),
                        proxyConfig.getMqttTlsCertificateFilePath(),
                        proxyConfig.getMqttTlsKeyFilePath(),
                        proxyConfig.getMqttTlsCiphers(),
                        proxyConfig.getMqttTlsProtocols(),
                        proxyConfig.isMqttTlsRequireTrustedClientCertOnConnect(),
                        proxyConfig.getMqttTlsCertRefreshCheckDurationSec());
            }
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(30, 0, 0));
        if (this.enableTls) {
            if (serverSslCtxRefresher != null) {
                SslContext sslContext = serverSslCtxRefresher.get();
                if (sslContext != null) {
                    ch.pipeline().addLast(TLS_HANDLER, sslContext.newHandler(ch.alloc()));
                }
            } else if (this.tlsEnabledWithKeyStore && serverSSLContextAutoRefreshBuilder != null) {
                ch.pipeline().addLast(TLS_HANDLER,
                        new SslHandler(serverSSLContextAutoRefreshBuilder.get().createSSLEngine()));
            }
        } else if (this.enableTlsPsk) {
            ch.pipeline().addLast(TLS_HANDLER,
                    new SslHandler(PSKUtils.createServerEngine(ch, proxyService.getPskConfiguration())));
        }
        ch.pipeline().addLast(MqttAdapterDecoder.NAME, new MqttAdapterDecoder(proxyConfig.getMqttMessageMaxLength()));
        ch.pipeline().addLast(MqttAdapterEncoder.NAME, MqttAdapterEncoder.INSTANCE);
        ch.pipeline().addLast("handler", new MQTTProxyInboundHandler(proxyService));
    }

}
