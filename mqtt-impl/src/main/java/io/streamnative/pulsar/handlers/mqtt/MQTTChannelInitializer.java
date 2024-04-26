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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.streamnative.pulsar.handlers.mqtt.adapter.CombineAdapterHandler;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterDecoder;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterEncoder;
import io.streamnative.pulsar.handlers.mqtt.codec.MqttWebSocketCodec;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKUtils;
import org.apache.pulsar.common.util.NettyServerSslContextBuilder;
import org.apache.pulsar.common.util.SslContextAutoRefreshBuilder;
import org.apache.pulsar.common.util.keystoretls.NettySSLContextAutoRefreshBuilder;

/**
 * A channel initializer that initialize channels for MQTT protocol.
 */
public class MQTTChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final MQTTServerConfiguration mqttConfig;
    private final MQTTService mqttService;
    private final boolean enableTls;
    private final boolean enableTlsPsk;
    private final boolean enableWs;
    private final boolean tlsEnabledWithKeyStore;

    private SslContextAutoRefreshBuilder<SslContext> sslCtxRefresher;
    private NettySSLContextAutoRefreshBuilder nettySSLContextAutoRefreshBuilder;

    public MQTTChannelInitializer(MQTTService mqttService, boolean enableTls, boolean enableWs) {
        this(mqttService, enableTls, false, enableWs);
    }

    public MQTTChannelInitializer(MQTTService mqttService, boolean enableTls, boolean enableTlsPsk, boolean enableWs) {
        super();
        this.mqttService = mqttService;
        this.mqttConfig = mqttService.getServerConfiguration();
        this.enableTls = enableTls;
        this.enableTlsPsk = enableTlsPsk;
        this.enableWs = enableWs;
        this.tlsEnabledWithKeyStore = mqttConfig.isMqttTlsEnabledWithKeyStore();
        if (this.enableTls) {
            if (tlsEnabledWithKeyStore) {
                nettySSLContextAutoRefreshBuilder = new NettySSLContextAutoRefreshBuilder(
                        mqttConfig.getMqttTlsProvider(),
                        mqttConfig.getMqttTlsKeyStoreType(),
                        mqttConfig.getMqttTlsKeyStore(),
                        mqttConfig.getMqttTlsKeyStorePassword(),
                        mqttConfig.isMqttTlsAllowInsecureConnection(),
                        mqttConfig.getMqttTlsTrustStoreType(),
                        mqttConfig.getMqttTlsTrustStore(),
                        mqttConfig.getMqttTlsTrustStorePassword(),
                        mqttConfig.isMqttTlsRequireTrustedClientCertOnConnect(),
                        mqttConfig.getMqttTlsCiphers(),
                        mqttConfig.getMqttTlsProtocols(),
                        mqttConfig.getMqttTlsCertRefreshCheckDurationSec());
            } else {
                sslCtxRefresher = new NettyServerSslContextBuilder(
                        null,
                        mqttConfig.isMqttTlsAllowInsecureConnection(),
                        mqttConfig.getMqttTlsTrustCertsFilePath(),
                        mqttConfig.getMqttTlsCertificateFilePath(),
                        mqttConfig.getMqttTlsKeyFilePath(),
                        mqttConfig.getMqttTlsCiphers(),
                        mqttConfig.getMqttTlsProtocols(),
                        mqttConfig.isMqttTlsRequireTrustedClientCertOnConnect(),
                        mqttConfig.getMqttTlsCertRefreshCheckDurationSec());
            }
        }
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(0, 0, 120));
        if (this.enableTls) {
            if (this.tlsEnabledWithKeyStore) {
                ch.pipeline().addLast(TLS_HANDLER,
                        new SslHandler(nettySSLContextAutoRefreshBuilder.get().createSSLEngine()));
            } else {
                ch.pipeline().addLast(TLS_HANDLER, sslCtxRefresher.get().newHandler(ch.alloc()));
            }
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
}
