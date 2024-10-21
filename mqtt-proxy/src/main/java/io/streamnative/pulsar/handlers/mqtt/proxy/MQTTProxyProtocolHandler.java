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

import static io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils.PROTOCOL_NAME;
import com.google.common.collect.ImmutableMap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.mqtt.common.utils.ConfigurationUtils;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
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
public class MQTTProxyProtocolHandler implements ProtocolHandler {

    @Getter
    private MQTTProxyConfiguration proxyConfig;

    @Getter
    private BrokerService brokerService;

    @Getter
    private String bindAddress;

    private MQTTProxyService proxyService;

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
        proxyConfig = ConfigurationUtils.create(conf.getProperties(), MQTTProxyConfiguration.class);
        // We have to enable ack batch message individual.
        proxyConfig.setAcknowledgmentAtBatchIndexLevelEnabled(true);
        this.bindAddress = ServiceConfigurationUtils.getDefaultOrConfiguredAddress(proxyConfig.getBindAddress());
    }

    @Override
    public String getProtocolDataToAdvertise() {
        return null;
    }

    @Override
    public void start(BrokerService brokerService) {
        this.brokerService = brokerService;
        try {
            proxyService = new MQTTProxyService(brokerService, proxyConfig);
            proxyService.start();
            log.info("Start MQTT proxy service at port: {}", proxyConfig.getMqttProxyPort());
        } catch (Exception ex) {
            log.error("Failed to start MQTT proxy service.", ex);
        }
    }

    @Override
    public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
        try {
            ImmutableMap.Builder<InetSocketAddress, ChannelInitializer<SocketChannel>> builder =
                    ImmutableMap.<InetSocketAddress, ChannelInitializer<SocketChannel>>builder();
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
