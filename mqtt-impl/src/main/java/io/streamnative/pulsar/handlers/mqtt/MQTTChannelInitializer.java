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

import static org.apache.pulsar.client.impl.PulsarChannelInitializer.TLS_HANDLER;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.support.DefaultProtocolMethodProcessorImpl;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTMetricsCollector;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKConfiguration;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKUtils;
import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;

/**
 * A channel initializer that initialize channels for MQTT protocol.
 */
public class MQTTChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Getter
    private final PulsarService pulsarService;
    @Getter
    private final MQTTServerConfiguration mqttConfig;

    private final Map<String, AuthenticationProvider> authProviders;
    private final AuthorizationService authorizationService;
    private final MQTTMetricsCollector metricsCollector;
    private final boolean enableTls;
    private final boolean enableTlsPsk;
    private final boolean tlsEnabledWithKeyStore;
    private SslContextAutoRefreshBuilder<SslContext> sslCtxRefresher;
    private NettySSLContextAutoRefreshBuilder nettySSLContextAutoRefreshBuilder;
    private PSKConfiguration pskConfiguration;

    public MQTTChannelInitializer(MQTTService mqttService,
                                  boolean enableTls) {
        this(mqttService, enableTls, false);
    }

    public MQTTChannelInitializer(MQTTService mqttService, boolean enableTls, boolean enableTlsPsk) {
        super();
        this.pulsarService = mqttService.getPulsarService();
        this.mqttConfig = mqttService.getServerConfiguration();
        this.authProviders = mqttService.getAuthProviders();
        this.authorizationService = mqttService.getAuthorizationService();
        this.metricsCollector = mqttService.getMetricsCollector();
        this.enableTls = enableTls;
        this.enableTlsPsk = enableTlsPsk;
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
        } else if (this.enableTlsPsk) {
            pskConfiguration = new PSKConfiguration();
            pskConfiguration.setIdentityHint(mqttConfig.getTlsPskIdentityHint());
            pskConfiguration.setIdentity(mqttConfig.getTlsPskIdentity());
            pskConfiguration.setIdentityFile(mqttConfig.getTlsPskIdentityFile());
            pskConfiguration.setProtocols(mqttConfig.getTlsProtocols());
            pskConfiguration.setCiphers(mqttConfig.getTlsCiphers());
        } else {
            this.sslCtxRefresher = null;
        }
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(30, 0, 0));
        if (this.enableTls) {
            if (this.tlsEnabledWithKeyStore) {
                ch.pipeline().addLast(TLS_HANDLER,
                        new SslHandler(nettySSLContextAutoRefreshBuilder.get().createSSLEngine()));
            } else {
                ch.pipeline().addLast(TLS_HANDLER, sslCtxRefresher.get().newHandler(ch.alloc()));
            }
        } else if (this.enableTlsPsk) {
            ch.pipeline().addLast(TLS_HANDLER, new SslHandler(PSKUtils.createServerEngine(ch, pskConfiguration)));
        }
        ch.pipeline().addLast("decoder", new MqttDecoder());
        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
        ch.pipeline().addLast("handler",
                new MQTTInboundHandler(new DefaultProtocolMethodProcessorImpl(pulsarService, mqttConfig,
                        authProviders, authorizationService, metricsCollector)));

    }
}
