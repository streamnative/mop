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

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.LISTENER_DEL;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.PROTOCOL_PROXY_NAME;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.PROXY_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.getProxyListenerPort;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils;
import io.streamnative.pulsar.handlers.mqtt.proxy.channel.MQTTProxyChannelInitializer;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.proxy.extensions.ProxyExtension;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

/**
 * MQTT Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class MQTTProxyProtocolHandler implements ProxyExtension {

    @Getter
    private MQTTProxyConfiguration proxyConfig;

    @Getter
    private ProxyConfiguration conf;

    @Getter
    private ProxyService proxyService;

    @Getter
    private String bindAddress;

    @Getter
    private String advertisedAddress;

    private MQTTProxyService mqttProxyService;

    private ScheduledExecutorService sslContextRefresher;

    private MQTTProxyServiceConfig proxyServiceConfig;

    @Override
    public String extensionName() {
        return PROTOCOL_PROXY_NAME;
    }

    @Override
    public boolean accept(String protocol) {
        return PROTOCOL_PROXY_NAME.equals(protocol.toLowerCase());
    }

    @Override
    public void initialize(ProxyConfiguration conf) throws Exception {
        this.conf = conf;
        this.proxyConfig = ConfigurationUtils.create(conf.getProperties(), MQTTProxyConfiguration.class);
        // We have to enable ack batch message individual.
        this.proxyConfig.setAcknowledgmentAtBatchIndexLevelEnabled(true);
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(conf.getBindAddress());
        this.advertisedAddress =
                ServiceConfigurationUtils.getDefaultOrConfiguredAddress(conf.getAdvertisedAddress());
    }

    @Override
    public void start(ProxyService service) {
        this.proxyService = service;
        try {
            this.proxyServiceConfig = initProxyConfig();
            this.mqttProxyService = new MQTTProxyService(proxyServiceConfig);
            this.mqttProxyService.start0();
            log.info("Start MQTT proxy service ");
        } catch (Exception ex) {
            log.error("Failed to start MQTT proxy service.", ex);
        }
    }

    private MQTTProxyServiceConfig initProxyConfig() throws Exception {
        MQTTProxyServiceConfig config = new MQTTProxyServiceConfig();
        config.setBindAddress(bindAddress);
        config.setAdvertisedAddress(advertisedAddress);
        config.setProxyConfiguration(proxyConfig);
        config.setLocalMetadataStore(createLocalMetadataStore());
        config.setConfigMetadataStore(createConfigurationMetadataStore());
        config.setPulsarResources(new PulsarResources(config.getLocalMetadataStore(), config.getConfigMetadataStore()));
        config.setPulsarClient(getClient());
        return config;
    }

    public MetadataStoreExtended createLocalMetadataStore() throws MetadataStoreException {
        return PulsarResources.createLocalMetadataStore(conf.getMetadataStoreUrl(),
                conf.getMetadataStoreSessionTimeoutMillis(),
                conf.isMetadataStoreAllowReadOnlyOperations());
    }

    public MetadataStoreExtended createConfigurationMetadataStore() throws MetadataStoreException {
        return PulsarResources.createConfigMetadataStore(conf.getConfigurationMetadataStoreUrl(),
                conf.getMetadataStoreSessionTimeoutMillis(),
                conf.isMetadataStoreAllowReadOnlyOperations());
    }

    private PulsarClientImpl getClient() throws Exception {
        final List<? extends ServiceLookupData> availableBrokers =
                proxyService.getDiscoveryProvider().getAvailableBrokers();
        if (availableBrokers.isEmpty()) {
            throw new PulsarServerException("No active broker is available");
        }
        final ServiceLookupData serviceLookupData = availableBrokers.get(0);
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(proxyConfig.isTlsEnabled()
                ? serviceLookupData.getPulsarServiceUrlTls() : serviceLookupData.getPulsarServiceUrl());
        conf.setTlsAllowInsecureConnection(proxyConfig.isTlsAllowInsecureConnection());
        conf.setTlsTrustCertsFilePath(proxyConfig.getTlsCertificateFilePath());

        if (proxyConfig.isBrokerClientTlsEnabled()) {
            if (proxyConfig.isBrokerClientTlsEnabledWithKeyStore()) {
                conf.setUseKeyStoreTls(true);
                conf.setTlsTrustStoreType(proxyConfig.getBrokerClientTlsTrustStoreType());
                conf.setTlsTrustStorePath(proxyConfig.getBrokerClientTlsTrustStore());
                conf.setTlsTrustStorePassword(proxyConfig.getBrokerClientTlsTrustStorePassword());
            } else {
                conf.setTlsTrustCertsFilePath(
                        isNotBlank(proxyConfig.getBrokerClientTrustCertsFilePath())
                                ? proxyConfig.getBrokerClientTrustCertsFilePath()
                                : proxyConfig.getTlsCertificateFilePath());
            }
        }

        try {
            if (isNotBlank(proxyConfig.getBrokerClientAuthenticationPlugin())) {
                conf.setAuthPluginClassName(proxyConfig.getBrokerClientAuthenticationPlugin());
                conf.setAuthParams(proxyConfig.getBrokerClientAuthenticationParameters());
                conf.setAuthParamMap(null);
                conf.setAuthentication(AuthenticationFactory.create(
                        proxyConfig.getBrokerClientAuthenticationPlugin(),
                        proxyConfig.getBrokerClientAuthenticationParameters()));
            }
            return new PulsarClientImpl(conf);
        } catch (PulsarClientException e) {
            log.error("Failed to create PulsarClient", e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        try {
            checkArgument(proxyConfig != null);
            checkArgument(proxyConfig.getMqttProxyListeners() != null);
            checkArgument(proxyService != null);

            this.sslContextRefresher = Executors.newSingleThreadScheduledExecutor(
                    new DefaultThreadFactory("mop-ssl-context-refresher"));

            String listeners = proxyConfig.getMqttProxyListeners();
            String[] parts = listeners.split(LISTENER_DEL);
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();

            for (String listener: parts) {
                if (listener.startsWith(PROXY_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(bindAddress, getProxyListenerPort(listener)),
                            new MQTTProxyChannelInitializer(
                                    mqttProxyService, proxyConfig, false, false, sslContextRefresher));
                }
            }
            return builder.build();
        } catch (Exception e) {
            log.error("MQTTProtocolHandler newChannelInitializers failed with", e);
            return null;
        }
    }

    @Override
    public void close() {
        if (sslContextRefresher != null) {
            sslContextRefresher.shutdownNow();
        }
        if (mqttProxyService != null) {
            mqttProxyService.close();
        }
    }

}
