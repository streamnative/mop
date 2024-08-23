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
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.adapter.CombineAdapterHandler;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterDecoder;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterEncoder;
import io.streamnative.pulsar.handlers.mqtt.codec.MqttWebSocketCodec;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKUtils;
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
            addWsHandler(ch.pipeline());
        }
        // Decoder
        ch.pipeline().addLast(MqttAdapterDecoder.NAME, new MqttAdapterDecoder());
        ch.pipeline().addLast("mqtt-decoder", new MqttDecoder(mqttConfig.getMqttMessageMaxLength()));
        // Encoder
        ch.pipeline().addLast(MqttAdapterEncoder.NAME, MqttAdapterEncoder.INSTANCE);
        // Handler
        ch.pipeline().addLast(CombineAdapterHandler.NAME, new CombineAdapterHandler());
        ch.pipeline().addLast(MQTTInboundHandler.NAME, new MQTTInboundHandler(mqttService));
    }

    /**
     * Add websocket handler.
     * @param pipeline
     */
    private void addWsHandler(ChannelPipeline pipeline) {
        // Encode or decode request and reply messages into HTTP messages
        pipeline.addLast(Constants.HANDLER_HTTP_CODEC, new HttpServerCodec());

        // Combine the parts of an HTTP message into a complete HTTP message
        pipeline.addLast(Constants.HANDLER_HTTP_AGGREGATOR,
                new HttpObjectAggregator(mqttConfig.getHttpMaxContentLength()));

        // Compress and encode HTTP messages
        pipeline.addLast(Constants.HANDLER_HTTP_COMPRESSOR, new HttpContentCompressor());

        pipeline.addLast(Constants.HANDLER_WEB_SOCKET_SERVER_PROTOCOL,
                new WebSocketServerProtocolHandler(mqttConfig.getWebSocketPath(), Constants.MQTT_SUB_PROTOCOL_CSV_LIST,
                        true, mqttConfig.getWebSocketMaxFrameSize()));
        pipeline.addLast(Constants.HANDLER_MQTT_WEB_SOCKET_CODEC, new MqttWebSocketCodec());
    }

    protected PulsarSslConfiguration buildSslConfiguration(MQTTServerConfiguration config) {
        return PulsarSslConfiguration.builder()
                .tlsProvider(config.getTlsProvider())
                .tlsKeyStoreType(config.getTlsKeyStoreType())
                .tlsKeyStorePath(config.getTlsKeyStore())
                .tlsKeyStorePassword(config.getTlsKeyStorePassword())
                .tlsTrustStoreType(config.getTlsTrustStoreType())
                .tlsTrustStorePath(config.getTlsTrustStore())
                .tlsTrustStorePassword(config.getTlsTrustStorePassword())
                .tlsCiphers(config.getTlsCiphers())
                .tlsProtocols(config.getTlsProtocols())
                .tlsTrustCertsFilePath(config.getTlsTrustCertsFilePath())
                .tlsCertificateFilePath(config.getTlsCertificateFilePath())
                .tlsKeyFilePath(config.getTlsKeyFilePath())
                .allowInsecureConnection(config.isTlsAllowInsecureConnection())
                .requireTrustedClientCertOnConnect(config.isTlsRequireTrustedClientCertOnConnect())
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
