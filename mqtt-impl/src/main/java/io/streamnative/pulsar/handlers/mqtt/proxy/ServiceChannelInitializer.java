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
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;

/**
 * Proxy service channel initializer.
 */
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final ProxyService proxyService;
    @Getter
    private final ProxyConfiguration proxyConfig;

    private final boolean enableTls;
    private final boolean tlsEnabledWithKeyStore;

    private SslContextAutoRefreshBuilder<SslContext> serverSslCtxRefresher;
    private NettySSLContextAutoRefreshBuilder serverSSLContextAutoRefreshBuilder;

    public ServiceChannelInitializer(ProxyService proxyService, ProxyConfiguration proxyConfig, boolean enableTls) {
        this.proxyService = proxyService;
        this.proxyConfig = proxyConfig;
        this.enableTls = enableTls;
        this.tlsEnabledWithKeyStore = proxyConfig.isTlsEnabledWithKeyStore();
        if (this.enableTls) {
            if (tlsEnabledWithKeyStore) {
                serverSSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                        proxyConfig.getTlsProvider(),
                        proxyConfig.getTlsKeyStoreType(),
                        proxyConfig.getTlsKeyStore(),
                        proxyConfig.getTlsKeyStorePassword(),
                        proxyConfig.isTlsAllowInsecureConnection(),
                        proxyConfig.getTlsTrustStoreType(),
                        proxyConfig.getTlsTrustStore(),
                        proxyConfig.getTlsTrustStorePassword(),
                        proxyConfig.isTlsRequireTrustedClientCertOnConnect(),
                        proxyConfig.getTlsCiphers(),
                        proxyConfig.getTlsProtocols(),
                        proxyConfig.getTlsCertRefreshCheckDurationSec());
            } else {
                serverSslCtxRefresher = new NettyServerSslContextBuilder(
                        proxyConfig.isTlsAllowInsecureConnection(),
                        proxyConfig.getTlsTrustCertsFilePath(),
                        proxyConfig.getTlsCertificateFilePath(),
                        proxyConfig.getTlsKeyFilePath(),
                        proxyConfig.getTlsCiphers(),
                        proxyConfig.getTlsProtocols(),
                        proxyConfig.isTlsRequireTrustedClientCertOnConnect(),
                        proxyConfig.getTlsCertRefreshCheckDurationSec());
            }
        } else {
            this.serverSslCtxRefresher = null;
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(10, 0, 0));
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
        }
        ch.pipeline().addLast("decoder", new MqttDecoder());
        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
        ch.pipeline().addLast("handler", new ProxyConnection(proxyService));
    }

}
