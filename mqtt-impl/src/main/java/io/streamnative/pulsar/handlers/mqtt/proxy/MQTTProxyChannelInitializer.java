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
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.CombineAdapterHandler;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterDecoder;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterEncoder;
import io.streamnative.pulsar.handlers.mqtt.common.psk.PSKUtils;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;

/**
 * Proxy service channel initializer.
 */
@Slf4j
public class MQTTProxyChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final MQTTProxyService proxyService;
    @Getter
    private final MQTTProxyConfiguration proxyConfig;

    private final boolean enableTls;
    private final boolean enableTlsPsk;
    private PulsarSslFactory sslFactory;

    public MQTTProxyChannelInitializer(MQTTProxyService proxyService, MQTTProxyConfiguration proxyConfig,
                                       boolean enableTls,
                                       ScheduledExecutorService sslContextRefresher) throws MQTTProxyException {
        this(proxyService, proxyConfig, enableTls, false, sslContextRefresher);
    }

    public MQTTProxyChannelInitializer(MQTTProxyService proxyService, MQTTProxyConfiguration proxyConfig,
                                       boolean enableTls, boolean enableTlsPsk,
                                       ScheduledExecutorService sslContextRefresher) throws MQTTProxyException {
        try {
            this.proxyService = proxyService;
            this.proxyConfig = proxyConfig;
            this.enableTls = enableTls;
            this.enableTlsPsk = enableTlsPsk;
            if (this.enableTls) {
                PulsarSslConfiguration sslConfiguration = buildSslConfiguration(proxyConfig);
                this.sslFactory = (PulsarSslFactory) Class.forName(proxyConfig.getSslFactoryPlugin())
                        .getConstructor().newInstance();
                this.sslFactory.initialize(sslConfiguration);
                this.sslFactory.createInternalSslContext();
                if (proxyConfig.getTlsCertRefreshCheckDurationSec() > 0) {
                    sslContextRefresher.scheduleWithFixedDelay(this::refreshSslContext,
                            proxyConfig.getTlsCertRefreshCheckDurationSec(),
                            proxyConfig.getTlsCertRefreshCheckDurationSec(), TimeUnit.SECONDS);

                }
            }
        } catch (Exception e) {
            throw new MQTTProxyException(e);
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(30, 0, 0));
        if (this.enableTls) {
            ch.pipeline().addLast(TLS_HANDLER, new SslHandler(sslFactory.createServerSslEngine(ch.alloc())));
        } else if (this.enableTlsPsk) {
            ch.pipeline().addLast(TLS_HANDLER,
                    new SslHandler(PSKUtils.createServerEngine(ch, proxyService.getPskConfiguration())));
        }
        ch.pipeline().addLast(MqttAdapterDecoder.NAME, new MqttAdapterDecoder());
        ch.pipeline().addLast("mqtt-decoder", new MqttDecoder(proxyConfig.getMqttMessageMaxLength()));
        // Encoder
        ch.pipeline().addLast(MqttAdapterEncoder.NAME, MqttAdapterEncoder.INSTANCE);
        // Handler
        ch.pipeline().addLast(CombineAdapterHandler.NAME, new CombineAdapterHandler());
        ch.pipeline().addLast("handler", new MQTTProxyInboundHandler(proxyService));
    }

    protected PulsarSslConfiguration buildSslConfiguration(MQTTProxyConfiguration config) {
        return PulsarSslConfiguration.builder()
                .tlsProvider(config.getMqttTlsProvider())
                .tlsKeyStoreType(config.getMqttTlsKeyStoreType())
                .tlsKeyStorePath(config.getMqttTlsKeyStore())
                .tlsKeyStorePassword(config.getMqttTlsKeyStorePassword())
                .tlsTrustStoreType(config.getMqttTlsTrustStoreType())
                .tlsTrustStorePath(config.getMqttTlsTrustStore())
                .tlsTrustStorePassword(config.getMqttTlsTrustStorePassword())
                .tlsCiphers(config.getMqttTlsCiphers())
                .tlsProtocols(config.getMqttTlsProtocols())
                .tlsTrustCertsFilePath(config.getMqttTlsTrustCertsFilePath())
                .tlsCertificateFilePath(config.getMqttTlsCertificateFilePath())
                .tlsKeyFilePath(config.getMqttTlsKeyFilePath())
                .allowInsecureConnection(config.isMqttTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(config.isMqttTlsRequireTrustedClientCertOnConnect())
                .tlsEnabledWithKeystore(config.isMqttTlsEnabledWithKeyStore())
                .tlsCustomParams(config.getSslFactoryPluginParams())
                .authData(null)
                .serverMode(true)
                .build();
    }

    protected void refreshSslContext() {
        try {
            this.sslFactory.update();
        } catch (Exception e) {
            log.error("Failed to refresh SSL context for mqtt proxy channel.", e);
        }
    }

}
