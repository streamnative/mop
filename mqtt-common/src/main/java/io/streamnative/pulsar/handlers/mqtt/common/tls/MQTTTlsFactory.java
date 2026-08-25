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
package io.streamnative.pulsar.handlers.mqtt.common.tls;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTCommonConfiguration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.broker.tls.TlsFactorySupport;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.common.tls.impl.TlsContextAcquisition;
import org.apache.pulsar.common.tls.impl.TlsSynthesisSpec;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * Bridges MoP's mqttTls* settings to Pulsar's PIP-478 TLS factory SPI.
 */
public class MQTTTlsFactory implements AutoCloseable {

    private PulsarTlsFactory tlsFactory;
    private TlsHandle<SslContext> tlsSubscription;
    private volatile SslContext tlsServerContext;

    public MQTTTlsFactory(MQTTCommonConfiguration config, TlsPurpose purpose,
                          ScheduledExecutorService sslContextRefresher) throws Exception {
        this.tlsFactory = TlsFactorySupport.createFactory(config.getTlsFactoryClassName(),
                FileBasedTlsFactory.class,
                () -> createDefaultFactory(config, purpose));
        try {
            TlsFactoryInitContext initContext = TlsFactorySupport.initContext(
                    TlsFactorySupport.parseFactoryConfig(config.getTlsFactoryConfig()),
                    sslContextRefresher, sslContextRefresher);
            TlsFactorySupport.initializeBlocking(this.tlsFactory, initContext);
            this.tlsSubscription = TlsContextAcquisition.acquireNettyContext(this.tlsFactory, purpose,
                            TlsSynthesisSpec.server(config.isMqttTlsRequireTrustedClientCertOnConnect()),
                            context -> this.tlsServerContext = context)
                    .get()
                    .orElseThrow(() -> new IllegalStateException(
                            "TLS factory supplied no Netty SslContext for purpose " + purpose));
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public SslHandler newServerSslHandler(ByteBufAllocator allocator) {
        return TlsContextAcquisition.withPinnedContext(
                () -> this.tlsServerContext, context -> context.newHandler(allocator));
    }

    @Override
    public void close() {
        TlsHandle<SslContext> subscription = this.tlsSubscription;
        if (subscription != null) {
            this.tlsSubscription = null;
            subscription.dispose();
        }
        PulsarTlsFactory factory = this.tlsFactory;
        if (factory != null) {
            this.tlsFactory = null;
            factory.close();
        }
    }

    private static PulsarTlsFactory createDefaultFactory(MQTTCommonConfiguration config, TlsPurpose purpose) {
        Map<TlsPurpose, TlsPolicy> policies = Map.of(purpose, createPolicy(config));
        FileBasedTlsFactorySettings settings = FileBasedTlsFactorySettings.builder()
                .requireTrustedClientCert(config.isMqttTlsRequireTrustedClientCertOnConnect())
                .refreshIntervalSeconds(FileBasedTlsFactorySettings.refreshIntervalSecondsFromConfig(
                        config.getMqttTlsCertRefreshCheckDurationSec()))
                .engineProvider(TlsFactorySupport.engineProvider(config.getMqttTlsProvider()))
                .build();
        return new FileBasedTlsFactory(policies, settings);
    }

    private static TlsPolicy createPolicy(MQTTCommonConfiguration config) {
        TlsPolicy.Builder builder = TlsPolicy.builder()
                .allowInsecureConnection(config.isMqttTlsAllowInsecureConnection())
                .enableHostnameVerification(config.isTlsHostnameVerificationEnabled())
                .protocols(toList(config.getMqttTlsProtocols()))
                .ciphers(toList(config.getMqttTlsCiphers()))
                .jsseProvider(TlsFactorySupport.resolveJsseProvider(config.getJsseProvider(),
                        config.getMqttTlsProvider()));
        if (config.isMqttTlsEnabledWithKeyStore()) {
            builder.format(TlsPolicy.Format.KEYSTORE)
                    .keyStoreType(config.getMqttTlsKeyStoreType())
                    .trustStoreType(config.getMqttTlsTrustStoreType())
                    .keyStorePath(config.getMqttTlsKeyStore())
                    .keyStorePassword(config.getMqttTlsKeyStorePassword())
                    .trustStorePath(config.getMqttTlsTrustStore())
                    .trustStorePassword(config.getMqttTlsTrustStorePassword());
        } else {
            builder.format(TlsPolicy.Format.PEM)
                    .trustCertsFilePath(config.getMqttTlsTrustCertsFilePath())
                    .certificateFilePath(config.getMqttTlsCertificateFilePath())
                    .keyFilePath(config.getMqttTlsKeyFilePath());
        }
        return builder.build();
    }

    private static List<String> toList(Set<String> values) {
        return values == null ? List.of() : List.copyOf(values);
    }
}
