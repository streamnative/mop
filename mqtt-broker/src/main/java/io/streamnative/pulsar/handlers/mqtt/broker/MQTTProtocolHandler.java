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
package io.streamnative.pulsar.handlers.mqtt.broker;

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.LISTENER_DEL;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.PLAINTEXT_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.PROTOCOL_NAME;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.SSL_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.SSL_PSK_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.WS_PLAINTEXT_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.WS_SSL_PREFIX;
import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.getListenerPort;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.mqtt.MopVersion;
import io.streamnative.pulsar.handlers.mqtt.broker.channel.MQTTChannelInitializer;
import io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyConfiguration;
import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyService;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.streamnative.pulsar.handlers.mqtt.proxy.MQTTProxyServiceConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.protocol.ProtocolHandler;
import org.apache.pulsar.broker.service.BrokerService;

/**
 * MQTT Protocol Handler load and run by Pulsar Service.
 */
@Slf4j
public class MQTTProtocolHandler implements ProtocolHandler {

    @Getter
    private MQTTServerConfiguration mqttConfig;

    @Getter
    private BrokerService brokerService;

    @Getter
    private String bindAddress;

    @Getter
    private String advertisedAddress;

    private MQTTProxyService proxyService;

    @Getter
    private MQTTService mqttService;

    private ScheduledExecutorService sslContextRefresher;

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
        this. mqttConfig = ConfigurationUtils.create(conf.getProperties(), MQTTServerConfiguration.class);
        // We have to enable ack batch message individual.
        this.mqttConfig.setAcknowledgmentAtBatchIndexLevelEnabled(true);
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(mqttConfig.getBindAddress());
        this.advertisedAddress =
                ServiceConfigurationUtils.getDefaultOrConfiguredAddress(mqttConfig.getAdvertisedAddress());
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
        mqttService = new MQTTService(brokerService, mqttConfig);
        if (mqttConfig.isMqttProxyEnabled() || mqttConfig.isMqttProxyEnable()) {
            try {
                final MQTTProxyServiceConfig config = initProxyConfig();
                proxyService = new MQTTProxyService(config);
                proxyService.start();
                log.info("Start MQTT proxy service at port: {}", config.getProxyConfiguration().getMqttProxyPort());
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

    private MQTTProxyServiceConfig initProxyConfig() throws Exception {
        MQTTProxyConfiguration proxyConfig =
                ConfigurationUtils.create(mqttConfig.getProperties(), MQTTProxyConfiguration.class);
        MQTTProxyServiceConfig config = new MQTTProxyServiceConfig();
        config.setBindAddress(bindAddress);
        config.setAdvertisedAddress(advertisedAddress);
        config.setProxyConfiguration(proxyConfig);
        config.setLocalMetadataStore(brokerService.pulsar().getLocalMetadataStore());
        config.setConfigMetadataStore(brokerService.pulsar().getConfigurationMetadataStore());
        config.setPulsarClient(brokerService.pulsar().getClient());
        return config;
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        checkArgument(mqttConfig != null);
        checkArgument(mqttConfig.getMqttListeners() != null);
        checkArgument(brokerService != null);

        this.sslContextRefresher = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("mop-ssl-context-refresher"));

        String listeners = mqttConfig.getMqttListeners();
        String[] parts = listeners.split(LISTENER_DEL);
        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();

            for (String listener: parts) {
                if (listener.startsWith(PLAINTEXT_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(mqttService, false, false, sslContextRefresher));

                } else if (listener.startsWith(SSL_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(mqttService, true, false, sslContextRefresher));

                } else if (listener.startsWith(SSL_PSK_PREFIX) && mqttConfig.isMqttTlsPskEnabled()) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(
                                    mqttService, false, true, false, sslContextRefresher));

                } else if (listener.startsWith(WS_PLAINTEXT_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(mqttService, false, true, sslContextRefresher));

                } else if (listener.startsWith(WS_SSL_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(mqttService, true, true, sslContextRefresher));

                } else {
                    log.error("MQTT listener {} not supported. supports {}, {} or {}",
                            listener, PLAINTEXT_PREFIX, SSL_PREFIX, SSL_PSK_PREFIX);
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
        if (proxyService != null) {
            proxyService.close();
        }
    }

}
