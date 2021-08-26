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
package io.streamnative.pulsar.handlers.mqtt;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.support.ProtocolMethodProcessorImpl;
import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;

/**
 * A channel initializer that initialize channels for MQTT protocol.
 */
public class MQTTChannelInitializer extends ChannelInitializer<SocketChannel> {
    public static final String TLS_HANDLER = "tls";
    @Getter

    private final PulsarService pulsarService;
    @Getter
    private final MQTTServerConfiguration mqttConfig;
    private final boolean enableTls;
    private final boolean tlsEnabledWithKeyStore;
    private SslContextAutoRefreshBuilder<SslContext> sslCtxRefresher;
    private NettySSLContextAutoRefreshBuilder nettySSLContextAutoRefreshBuilder;

    private final Map<String, AuthenticationProvider> authProviders;

    public MQTTChannelInitializer(PulsarService pulsarService,
                                  MQTTServerConfiguration mqttConfig,
                                  Map<String, AuthenticationProvider> authProviders,
                                  boolean enableTls
    )
            throws Exception {
        super();
        this.pulsarService = pulsarService;
        this.mqttConfig = mqttConfig;
        this.authProviders = authProviders;
        this.enableTls = enableTls;
        this.tlsEnabledWithKeyStore = mqttConfig.isTlsEnabledWithKeyStore();
        if (this.enableTls) {
            if (tlsEnabledWithKeyStore) {
                nettySSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                    mqttConfig.getTlsProvider(),
                    mqttConfig.getTlsKeyStoreType(),
                    mqttConfig.getTlsKeyStore(),
                    mqttConfig.getTlsKeyStorePassword(),
                    mqttConfig.isTlsAllowInsecureConnection(),
                    mqttConfig.getTlsTrustStoreType(),
                    mqttConfig.getTlsTrustStore(),
                    mqttConfig.getTlsTrustStorePassword(),
                    mqttConfig.isTlsRequireTrustedClientCertOnConnect(),
                    mqttConfig.getTlsCiphers(),
                    mqttConfig.getTlsProtocols(),
                    mqttConfig.getTlsCertRefreshCheckDurationSec());
            } else {
                sslCtxRefresher = new NettyServerSslContextBuilder(
                    mqttConfig.isTlsAllowInsecureConnection(),
                    mqttConfig.getTlsTrustCertsFilePath(),
                    mqttConfig.getTlsCertificateFilePath(),
                    mqttConfig.getTlsKeyFilePath(),
                    mqttConfig.getTlsCiphers(),
                    mqttConfig.getTlsProtocols(),
                    mqttConfig.isTlsRequireTrustedClientCertOnConnect(),
                    mqttConfig.getTlsCertRefreshCheckDurationSec());
            }
        } else {
            this.sslCtxRefresher = null;
        }
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(10, 0, 0));
        if (this.enableTls) {
            if (this.tlsEnabledWithKeyStore) {
                ch.pipeline().addLast(TLS_HANDLER,
                                      new SslHandler(nettySSLContextAutoRefreshBuilder.get().createSSLEngine()));
            } else {
                ch.pipeline().addLast(TLS_HANDLER, sslCtxRefresher.get().newHandler(ch.alloc()));
            }
        }
        ch.pipeline().addLast("decoder", new MqttDecoder());
        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
        ch.pipeline().addLast("handler",
                new MQTTInboundHandler(new ProtocolMethodProcessorImpl(pulsarService, mqttConfig, authProviders)));
    }
}
