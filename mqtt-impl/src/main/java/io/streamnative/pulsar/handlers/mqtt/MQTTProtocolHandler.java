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

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.LISTENER_DEL;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.PLAINTEXT_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.PROTOCOL_NAME;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.SSL_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.SSL_PSK_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils.getListenerPort;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyConfiguration;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import io.streamnative.pulsar.handlers.mqtt.utils.AuthUtils;
import io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils;
import java.net.InetSocketAddress;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authorization.AuthorizationProvider;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;
/**
 * MQTT Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class MQTTProtocolHandler implements ProtocolHandler {

    private Map<String, AuthenticationProvider> authProviders;

    private AuthorizationProvider authzProvider;

    @Getter
    private MQTTServerConfiguration mqttConfig;

    @Getter
    private BrokerService brokerService;

    @Getter
    private String bindAddress;

    private MQTTProxyService proxyService;

    private MQTTService mqttService;

    @Override
    public String protocolName() {
        return PROTOCOL_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_NAME.equals(protocol.toLowerCase());
    }

    @Override
    public void initialize(ServiceConfiguration conf) throws Exception {
        // init config
        if (conf instanceof MQTTServerConfiguration) {
            // in unit test, passed in conf will be MQTTServerConfiguration
            mqttConfig = (MQTTServerConfiguration) conf;
        } else {
            // when loaded with PulsarService as NAR, `conf` will be type of ServiceConfiguration
            mqttConfig = ConfigurationUtils.create(conf.getProperties(), MQTTServerConfiguration.class);
        }
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(mqttConfig.getBindAddress());
    }

    @Override
    public String getProtocolDataToAdvertise() {
        if (log.isDebugEnabled()) {
            log.debug("Get configured listener: {}", mqttConfig.getMqttListeners());
        }
        return mqttConfig.getMqttListeners();
    }

    @Override
    public void start(BrokerService brokerService) {
        this.brokerService = brokerService;
        if (mqttConfig.isMqttAuthenticationEnabled()) {
            this.authProviders = AuthUtils.configureAuthProviders(brokerService.getAuthenticationService(),
                                                                  mqttConfig.getMqttAuthenticationMethods());
        }
        if (mqttConfig.isMqttAuthorizationEnabled()) {
            this.authzProvider = AuthUtils.configureAuthzProvider(brokerService.getAuthorizationService(),
                    mqttConfig.getMqttAuthorizationMethod());
        }
        mqttService = new MQTTService(brokerService.pulsar(), mqttConfig, authProviders, authzProvider);
        if (mqttConfig.isMqttProxyEnable()) {
            MQTTProxyConfiguration proxyConfig = new MQTTProxyConfiguration();
            proxyConfig.setDefaultTenant(mqttConfig.getDefaultTenant());
            proxyConfig.setDefaultTopicDomain(mqttConfig.getDefaultTopicDomain());
            proxyConfig.setMaxNoOfChannels(mqttConfig.getMaxNoOfChannels());
            proxyConfig.setMaxFrameSize(mqttConfig.getMaxFrameSize());
            proxyConfig.setMqttProxyPort(mqttConfig.getMqttProxyPort());
            proxyConfig.setMqttProxyTlsPort(mqttConfig.getMqttProxyTlsPort());
            proxyConfig.setMqttProxyTlsPskPort(mqttConfig.getMqttProxyTlsPskPort());
            proxyConfig.setMqttProxyTlsEnabled(mqttConfig.isMqttProxyTlsEnabled());
            proxyConfig.setMqttProxyTlsPskEnabled(mqttConfig.isMqttProxyTlsPskEnabled());
            proxyConfig.setBrokerServiceURL("pulsar://"
                    + ServiceConfigurationUtils.getAppliedAdvertisedAddress(mqttConfig, true)
                    + ":" + mqttConfig.getBrokerServicePort().get());
            proxyConfig.setMqttProxyNumAcceptorThreads(mqttConfig.getMqttProxyNumAcceptorThreads());
            proxyConfig.setMqttProxyNumIOThreads(mqttConfig.getMqttProxyNumIOThreads());
            proxyConfig.setMqttAuthenticationEnabled(mqttConfig.isMqttAuthenticationEnabled());
            proxyConfig.setMqttAuthenticationMethods(mqttConfig.getMqttAuthenticationMethods());
            proxyConfig.setMqttAuthorizationEnabled(mqttConfig.isMqttAuthorizationEnabled());
            proxyConfig.setMqttAuthorizationMethod(mqttConfig.getMqttAuthorizationMethod());
            proxyConfig.setBrokerClientAuthenticationPlugin(mqttConfig.getBrokerClientAuthenticationPlugin());
            proxyConfig.setBrokerClientAuthenticationParameters(mqttConfig.getBrokerClientAuthenticationParameters());

            proxyConfig.setTlsCertificateFilePath(mqttConfig.getTlsCertificateFilePath());
            proxyConfig.setTlsCertRefreshCheckDurationSec(mqttConfig.getTlsCertRefreshCheckDurationSec());
            proxyConfig.setTlsProtocols(mqttConfig.getTlsProtocols());
            proxyConfig.setTlsCiphers(mqttConfig.getTlsCiphers());
            proxyConfig.setTlsAllowInsecureConnection(mqttConfig.isTlsAllowInsecureConnection());

            proxyConfig.setTlsPskIdentityHint(mqttConfig.getTlsPskIdentityHint());
            proxyConfig.setTlsPskIdentity(mqttConfig.getTlsPskIdentity());
            proxyConfig.setTlsPskIdentityFile(mqttConfig.getTlsPskIdentityFile());

            proxyConfig.setTlsTrustStore(mqttConfig.getTlsTrustStore());
            proxyConfig.setTlsTrustCertsFilePath(mqttConfig.getTlsTrustCertsFilePath());
            proxyConfig.setTlsTrustStoreType(mqttConfig.getTlsTrustStoreType());
            proxyConfig.setTlsTrustStorePassword(mqttConfig.getTlsTrustStorePassword());

            proxyConfig.setTlsEnabledWithKeyStore(mqttConfig.isTlsEnabledWithKeyStore());
            proxyConfig.setTlsKeyStore(mqttConfig.getTlsKeyStore());
            proxyConfig.setTlsKeyStoreType(mqttConfig.getTlsKeyStoreType());
            proxyConfig.setTlsKeyStorePassword(mqttConfig.getTlsTrustStorePassword());
            proxyConfig.setTlsKeyFilePath(mqttConfig.getTlsKeyFilePath());
            log.info("proxyConfig broker service URL: {}", proxyConfig.getBrokerServiceURL());
            proxyService = new MQTTProxyService(proxyConfig, mqttService);
            try {
                proxyService.start();
                log.info("Start MQTT proxy service at port: {}", proxyConfig.getMqttProxyPort());
            } catch (Exception ex) {
                log.error("Failed to start MQTT proxy service.", ex);
            }
        }

        log.info("Starting MqttProtocolHandler, MoP version is: '{}'", MopVersion.getVersion());
        log.info("Git Revision {}", MopVersion.getGitSha());
        log.info("Built by {} on {} at {}",
                MopVersion.getBuildUser(),
                MopVersion.getBuildHost(),
                MopVersion.getBuildTime());
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkArgument(mqttConfig != null);
        checkArgument(mqttConfig.getMqttListeners() != null);
        checkArgument(brokerService != null);

        String listeners = mqttConfig.getMqttListeners();
        String[] parts = listeners.split(LISTENER_DEL);
        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();

            for (String listener: parts) {
                if (listener.startsWith(PLAINTEXT_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(mqttService, false));

                } else if (listener.startsWith(SSL_PREFIX) && mqttConfig.isTlsEnabled()) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(mqttService, true));

                } else if (listener.startsWith(SSL_PSK_PREFIX) && mqttConfig.isTlsPskEnabled()) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(mqttService, false, true));

                } else {
                    log.error("MQTT listener {} not supported. supports {}, {} or {}",
                            listener, PLAINTEXT_PREFIX, SSL_PREFIX, SSL_PSK_PREFIX);
                }
            }

            return builder.build();
        } catch (Exception e){
            log.error("MQTTProtocolHandler newChannelInitializers failed with", e);
            return null;
        }
    }

    @Override
    public void close() {
        if (proxyService != null) {
            proxyService.close();
        }
    }

}
