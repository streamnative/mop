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

import static com.google.common.base.Preconditions.checkState;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.mqtt.proxy.ProxyConfiguration;
import io.streamnative.pulsar.handlers.mqtt.proxy.ProxyService;
import io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils;
import java.net.InetSocketAddress;
import java.util.Map;
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

    public static final String PROTOCOL_NAME = "mqtt";
    public static final String PLAINTEXT_PREFIX = "mqtt://";
    public static final String LISTENER_DEL = ",";
    public static final String LISTENER_PATTEN = "^(mqtt)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-0-9+]";

    @Getter
    private MQTTServerConfiguration mqttConfig;

    @Getter
    private BrokerService brokerService;

    @Getter
    private String bindAddress;

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
            // in unit test, passed in conf will be AmqpServiceConfiguration
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
            log.debug("Get configured listener", mqttConfig.getMqttListeners());
        }
        return mqttConfig.getMqttListeners();
    }

    @Override
    public void start(BrokerService brokerService) {
        this.brokerService = brokerService;

        if (mqttConfig.isMqttProxyEnable()) {
            ProxyConfiguration proxyConfig = new ProxyConfiguration();
            proxyConfig.setMqttTenant(mqttConfig.getDefaultTenant());
            proxyConfig.setMqttMaxNoOfChannels(mqttConfig.getMaxNoOfChannels());
            proxyConfig.setMqttMaxFrameSize(mqttConfig.getMaxFrameSize());
            proxyConfig.setMqttHeartBeat(mqttConfig.getHeartBeat());
            proxyConfig.setMqttProxyPort(mqttConfig.getMqttProxyPort());
            proxyConfig.setBrokerServiceURL("pulsar://"
                    + ServiceConfigurationUtils.getAppliedAdvertisedAddress(mqttConfig)
                    + ":" + mqttConfig.getBrokerServicePort().get());
            log.info("proxyConfig broker service URL: {}", proxyConfig.getBrokerServiceURL());
            ProxyService proxyService = new ProxyService(proxyConfig, brokerService.getPulsar());
            try {
                proxyService.start();
                log.info("Start MQTT proxy service at port: {}", proxyConfig.getMqttProxyPort());
            } catch (Exception e) {
                log.error("Failed to start MQTT proxy service.");
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
        checkState(mqttConfig != null);
        checkState(mqttConfig.getMqttListeners() != null);
        checkState(brokerService != null);

        String listeners = mqttConfig.getMqttListeners();
        String[] parts = listeners.split(LISTENER_DEL);
        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();

            for (String listener: parts) {
                if (listener.startsWith(PLAINTEXT_PREFIX)) {
                    builder.put(
                            new InetSocketAddress(brokerService.pulsar().getBindAddress(), getListenerPort(listener)),
                            new MQTTChannelInitializer(brokerService.pulsar(),
                                    mqttConfig));
                } else {
                    log.error("MQTT listener {} not supported. supports {}",
                            listener, PLAINTEXT_PREFIX);
                }
            }

            return builder.build();
        } catch (Exception e){
            log.error("AmqpProtocolHandler newChannelInitializers failed with", e);
            return null;
        }
    }

    @Override
    public void close() {

    }

    public static int getListenerPort(String listener) {
        checkState(listener.matches(LISTENER_PATTEN), "listener not match patten");

        int lastIndex = listener.lastIndexOf(':');
        return Integer.parseInt(listener.substring(lastIndex + 1));
    }
}
