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
package io.streamnative.pulsar.handlers.mqtt.broker.channel;

import static org.apache.pulsar.client.impl.PulsarChannelInitializer.TLS_HANDLER;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.broker.MQTTServerConfiguration;
import io.streamnative.pulsar.handlers.mqtt.broker.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.CombineAdapterHandler;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterDecoder;
import io.streamnative.pulsar.handlers.mqtt.common.adapter.MqttAdapterEncoder;
import io.streamnative.pulsar.handlers.mqtt.common.psk.PSKUtils;
import io.streamnative.pulsar.handlers.mqtt.common.utils.WebSocketUtils;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.PulsarSslConfiguration;
import org.apache.pulsar.common.util.PulsarSslFactory;

/**
 * A channel initializer that initialize channels for MQTT protocol.
 */
@Slf4j
public class MQTTChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final MQTTServerConfiguration mqttConfig;
    private final MQTTService mqttService;
    private final boolean enableTls;
    private final boolean enableTlsPsk;
    private final boolean enableWs;
    private PulsarSslFactory sslFactory;

    public MQTTChannelInitializer(MQTTService mqttService, boolean enableTls, boolean enableWs,
                                  ScheduledExecutorService sslContextRefresher) throws Exception {
        this(mqttService, enableTls, false, enableWs, sslContextRefresher);
    }

    public MQTTChannelInitializer(
            MQTTService mqttService, boolean enableTls, boolean enableTlsPsk, boolean enableWs,
            ScheduledExecutorService sslContextRefresher) throws Exception {
        super();
        this.mqttService = mqttService;
        this.mqttConfig = mqttService.getServerConfiguration();
        this.enableTls = enableTls;
        this.enableTlsPsk = enableTlsPsk;
        this.enableWs = enableWs;
        if (this.enableTls) {
            PulsarSslConfiguration sslConfiguration = buildSslConfiguration(mqttConfig);
            this.sslFactory = (PulsarSslFactory) Class.forName(mqttConfig.getSslFactoryPlugin())
                    .getConstructor().newInstance();
            this.sslFactory.initialize(sslConfiguration);
            this.sslFactory.createInternalSslContext();
            if (mqttConfig.getTlsCertRefreshCheckDurationSec() > 0) {
                sslContextRefresher.scheduleWithFixedDelay(this::refreshSslContext,
                        mqttConfig.getTlsCertRefreshCheckDurationSec(),
                        mqttConfig.getTlsCertRefreshCheckDurationSec(), TimeUnit.SECONDS);
            }
        }
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(0, 0, 120));
        if (this.enableTls) {
            ch.pipeline().addLast(TLS_HANDLER, new SslHandler(sslFactory.createServerSslEngine(ch.alloc())));
        } else if (this.enableTlsPsk) {
            ch.pipeline().addLast(TLS_HANDLER,
                    new SslHandler(PSKUtils.createServerEngine(ch, mqttService.getPskConfiguration())));
        }
        if (this.enableWs) {
            WebSocketUtils.addWsHandler(ch.pipeline(), mqttConfig);
        }
        // Decoder
        ch.pipeline().addLast(MqttAdapterDecoder.NAME, new MqttAdapterDecoder());
        ch.pipeline().addLast("mqtt-decoder", new MqttDecoder(mqttConfig.getMqttMessageMaxLength()));
        // Encoder
        ch.pipeline().addLast(MqttAdapterEncoder.NAME, MqttAdapterEncoder.INSTANCE);
        // Handler
        ch.pipeline().addLast(CombineAdapterHandler.NAME, new CombineAdapterHandler());
        ch.pipeline().addLast(MQTTBrokerInboundHandler.NAME, new MQTTBrokerInboundHandler(mqttService));
    }

    protected PulsarSslConfiguration buildSslConfiguration(MQTTServerConfiguration config) {
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
            log.error("Failed to refresh SSL context for mqtt channel.", e);
        }
    }

}
