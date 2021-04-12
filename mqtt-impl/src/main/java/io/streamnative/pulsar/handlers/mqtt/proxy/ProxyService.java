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
import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;

/**
 * This service is used for redirecting MQTT client request to proper MQTT protocol handler Broker.
 */
@Slf4j
public class ProxyService implements Closeable {

    @Getter
    private ProxyConfiguration proxyConfig;
    private String serviceUrl;
    @Getter
    private PulsarService pulsarService;
    @Getter
    private PulsarClientImpl pulsarClient;
    @Getter
    private LookupHandler lookupHandler;

    private Channel listenChannel;
    private EventLoopGroup acceptorGroup;
    @Getter
    private EventLoopGroup workerGroup;

    private DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("mqtt-redirect-acceptor");
    private DefaultThreadFactory workerThreadFactory = new DefaultThreadFactory("mqtt-redirect-io");
    private static final int numThreads = Runtime.getRuntime().availableProcessors();

    private ZooKeeperClientFactory zkClientFactory = null;

    private String tenant;

    public ProxyService(ProxyConfiguration proxyConfig, PulsarService pulsarService) {
        configValid(proxyConfig);

        this.proxyConfig = proxyConfig;
        this.pulsarService = pulsarService;
        this.tenant = this.proxyConfig.getMqttTenant();
        acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workerThreadFactory);
    }

    private void configValid(ProxyConfiguration proxyConfig) {
        checkNotNull(proxyConfig);
        checkArgument(proxyConfig.getMqttProxyPort() > 0);
        checkNotNull(proxyConfig.getMqttTenant());
        checkNotNull(proxyConfig.getBrokerServiceURL());
    }

    public void start() throws Exception {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptorGroup, workerGroup);
        serverBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(serverBootstrap);
        serverBootstrap.childHandler(new ServiceChannelInitializer(this));
        try {
            listenChannel = serverBootstrap.bind(proxyConfig.getMqttProxyPort()).sync().channel();
        } catch (InterruptedException e) {
            throw new IOException("Failed to bind Pulsar Proxy on port " + proxyConfig.getMqttProxyPort(), e);
        }

        this.pulsarClient = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(proxyConfig.getBrokerServiceURL())
                .build();

        this.lookupHandler = new PulsarServiceLookupHandler(pulsarService, pulsarClient);
    }

    @Override
    public void close() throws IOException {
        if (listenChannel != null) {
            listenChannel.close();
        }
    }
}
