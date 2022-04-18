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
import io.streamnative.pulsar.handlers.mqtt.MQTTAuthenticationService;
import io.streamnative.pulsar.handlers.mqtt.MQTTConnectionManager;
import io.streamnative.pulsar.handlers.mqtt.MQTTService;
import io.streamnative.pulsar.handlers.mqtt.adapter.MQTTProxyAdapter;
import io.streamnative.pulsar.handlers.mqtt.support.event.PulsarEventCenter;
import io.streamnative.pulsar.handlers.mqtt.support.event.PulsarEventCenterImpl;
import io.streamnative.pulsar.handlers.mqtt.support.psk.PSKConfiguration;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.DisabledSystemEventService;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.SystemEventService;
import io.streamnative.pulsar.handlers.mqtt.support.systemtopic.SystemTopicBasedSystemEventService;
import java.io.Closeable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

/**
 * This service is used for redirecting MQTT client request to proper MQTT protocol handler Broker.
 */
@Slf4j
public class MQTTProxyService implements Closeable {

    @Getter
    private final MQTTProxyConfiguration proxyConfig;
    @Getter
    private final PulsarService pulsarService;
    @Getter
    private final MQTTAuthenticationService authenticationService;
    @Getter
    private final MQTTConnectionManager connectionManager;
    @Getter
    private final SystemEventService eventService;
    @Getter
    private LookupHandler lookupHandler;
    @Getter
    private final PulsarEventCenter eventCenter;
    @Getter
    private final PSKConfiguration pskConfiguration;
    @Getter
    private final MQTTProxyAdapter proxyAdapter;

    private Channel listenChannel;
    private Channel listenChannelTls;
    private Channel listenChannelTlsPsk;
    private final EventLoopGroup acceptorGroup;
    private final EventLoopGroup workerGroup;

    private DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("mqtt-redirect-acceptor");
    private DefaultThreadFactory workerThreadFactory = new DefaultThreadFactory("mqtt-redirect-io");

    public MQTTProxyService(MQTTService mqttService, MQTTProxyConfiguration proxyConfig) {
        configValid(proxyConfig);
        this.pulsarService = mqttService.getPulsarService();
        this.proxyConfig = proxyConfig;
        this.pskConfiguration = new PSKConfiguration(proxyConfig);
        this.authenticationService = mqttService.getAuthenticationService();
        this.connectionManager = new MQTTConnectionManager(pulsarService.getAdvertisedAddress());
        this.eventService = proxyConfig.isSystemEventEnabled()
                ? new SystemTopicBasedSystemEventService(mqttService.getPulsarService()) :
                new DisabledSystemEventService();
        this.eventService.addListener(connectionManager.getEventListener());
        this.eventService.addListener(mqttService.getWillMessageHandler().getEventListener());
        this.eventService.addListener(mqttService.getRetainedMessageHandler().getEventListener());
        mqttService.setEventService(eventService);
        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(proxyConfig.getMqttProxyNumAcceptorThreads(),
                false, acceptorThreadFactory);
        this.workerGroup = EventLoopUtil.newEventLoopGroup(proxyConfig.getMqttProxyNumIOThreads(),
                false, workerThreadFactory);
        this.eventCenter = new PulsarEventCenterImpl(mqttService.getBrokerService(),
                proxyConfig.getEventCenterCallbackPoolThreadNum());
        this.proxyAdapter = new MQTTProxyAdapter(this);
    }

    private void configValid(MQTTProxyConfiguration proxyConfig) {
        checkNotNull(proxyConfig);
        checkArgument(proxyConfig.getMqttProxyPort() > 0);
        checkNotNull(proxyConfig.getDefaultTenant());
    }

    public void start() throws MQTTProxyException {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptorGroup, workerGroup);
        serverBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(serverBootstrap);
        serverBootstrap.childHandler(new MQTTProxyChannelInitializer(this, proxyConfig, false));

        try {
            listenChannel = serverBootstrap.bind(proxyConfig.getMqttProxyPort()).sync().channel();
            log.info("Started MQTT Proxy on {}", listenChannel.localAddress());
        } catch (InterruptedException e) {
            throw new MQTTProxyException(e);
        }

        if (proxyConfig.isMqttProxyTlsEnabled()) {
            ServerBootstrap tlsBootstrap = serverBootstrap.clone();
            tlsBootstrap.childHandler(new MQTTProxyChannelInitializer(this, proxyConfig, true));
            try {
                listenChannelTls = tlsBootstrap.bind(proxyConfig.getMqttProxyTlsPort()).sync().channel();
                log.info("Started MQTT Proxy with TLS on {}", listenChannelTls.localAddress());
            } catch (InterruptedException e) {
                throw new MQTTProxyException(e);
            }
        }

        if (proxyConfig.isMqttProxyTlsPskEnabled()) {
            // init psk config
            pskConfiguration.setIdentityHint(proxyConfig.getTlsPskIdentityHint());
            pskConfiguration.setIdentity(proxyConfig.getTlsPskIdentity());
            pskConfiguration.setIdentityFile(proxyConfig.getTlsPskIdentityFile());
            pskConfiguration.setProtocols(proxyConfig.getTlsProtocols());
            pskConfiguration.setCiphers(proxyConfig.getTlsCiphers());
            this.eventService.addListener(pskConfiguration.getEventListener());
            // Add channel initializer
            ServerBootstrap tlsPskBootstrap = serverBootstrap.clone();
            tlsPskBootstrap.childHandler(new MQTTProxyChannelInitializer(this, proxyConfig, false, true));
            try {
                listenChannelTlsPsk = tlsPskBootstrap.bind(proxyConfig.getMqttProxyTlsPskPort()).sync().channel();
                log.info("Started MQTT Proxy with TLS-PSK on {}", listenChannelTlsPsk.localAddress());
            } catch (InterruptedException e) {
                throw new MQTTProxyException(e);
            }
        }

        this.lookupHandler = new PulsarServiceLookupHandler(pulsarService, proxyConfig);
        this.eventService.start();
    }

    @Override
    public void close() {
        if (listenChannel != null) {
            listenChannel.close();
        }
        if (listenChannelTls != null) {
            listenChannelTls.close();
        }
        if (listenChannelTlsPsk != null) {
            listenChannelTlsPsk.close();
        }
        this.acceptorGroup.shutdownGracefully();
        this.workerGroup.shutdownGracefully();
        this.eventService.close();
        if (lookupHandler != null) {
            this.lookupHandler.close();
        }
        this.proxyAdapter.shutdown();
        this.connectionManager.close();
    }
}
