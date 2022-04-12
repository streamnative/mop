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
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.timeout.IdleStateHandler;
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
    private final boolean tlsEnabledWithKeyStore;

    private SslContextAutoRefreshBuilder<SslContext> sslCtxRefresher;
    private NettySSLContextAutoRefreshBuilder nettySSLContextAutoRefreshBuilder;

    public MQTTChannelInitializer(MQTTService mqttService, boolean enableTls) {
        this(mqttService, enableTls, false);
    }

    public MQTTChannelInitializer(MQTTService mqttService, boolean enableTls, boolean enableTlsPsk) {
        super();
        this.mqttService = mqttService;
        this.mqttConfig = mqttService.getServerConfiguration();
        this.enableTls = enableTls;
        this.enableTlsPsk = enableTlsPsk;
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
                SslProvider sslProvider = null;
                if (mqttConfig.getTlsProvider() != null) {
                    sslProvider = SslProvider.valueOf(mqttConfig.getTlsProvider());
                }
                sslCtxRefresher = new NettyServerSslContextBuilder(
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
        ch.pipeline().addFirst("idleStateHandler", new IdleStateHandler(30, 0, 0));
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
        ch.pipeline().addLast("decoder", new MqttDecoder(mqttConfig.getMqttMessageMaxLength()));
        ch.pipeline().addLast("encoder", MqttEncoder.INSTANCE);
        ch.pipeline().addLast("handler", new MQTTInboundHandler(mqttService));
    }
}
