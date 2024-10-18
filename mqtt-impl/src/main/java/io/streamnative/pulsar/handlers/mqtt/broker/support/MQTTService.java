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
package io.streamnative.pulsar.handlers.mqtt.broker.support;

import io.streamnative.pulsar.handlers.mqtt.common.authentication.MQTTAuthenticationService;
import io.streamnative.pulsar.handlers.mqtt.broker.metric.MQTTMetricsCollector;
import io.streamnative.pulsar.handlers.mqtt.broker.metric.MQTTMetricsProvider;
import io.streamnative.pulsar.handlers.mqtt.broker.feature.RetainedMessageHandler;
import io.streamnative.pulsar.handlers.mqtt.broker.feature.WillMessageHandler;
import io.streamnative.pulsar.handlers.mqtt.common.event.DisableEventCenter;
import io.streamnative.pulsar.handlers.mqtt.common.event.PulsarEventCenter;
import io.streamnative.pulsar.handlers.mqtt.common.event.PulsarEventCenterImpl;
import io.streamnative.pulsar.handlers.mqtt.common.psk.PSKConfiguration;
import io.streamnative.pulsar.handlers.mqtt.common.systemtopic.SystemEventService;
import io.streamnative.pulsar.handlers.mqtt.common.MQTTConnectionManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.BrokerService;

/**
 * Main class for mqtt service.
 */
@Slf4j
public class MQTTService {

    @Getter
    private final BrokerService brokerService;

    @Getter
    private final MQTTServerConfiguration serverConfiguration;

    @Getter
    private final PSKConfiguration pskConfiguration;

    @Getter
    private final PulsarService pulsarService;

    @Getter
    private final MQTTAuthenticationService authenticationService;

    @Getter
    private final AuthorizationService authorizationService;

    @Getter
    private final MQTTMetricsProvider metricsProvider;

    @Getter
    private final MQTTMetricsCollector metricsCollector;

    @Getter
    private final MQTTConnectionManager connectionManager;

    @Getter
    private final MQTTSubscriptionManager subscriptionManager;

    @Getter
    private final MQTTNamespaceBundleOwnershipListener bundleOwnershipListener;

    @Getter
    private final PulsarEventCenter eventCenter;

    @Getter
    private final WillMessageHandler willMessageHandler;

    @Getter
    private final RetainedMessageHandler retainedMessageHandler;

    @Getter
    private final QosPublishHandlers qosPublishHandlers;

    @Getter
    @Setter
    private SystemEventService eventService;

    public MQTTService(BrokerService brokerService, MQTTServerConfiguration serverConfiguration) {
        this.brokerService = brokerService;
        this.pulsarService = brokerService.pulsar();
        this.serverConfiguration = serverConfiguration;
        this.pskConfiguration = new PSKConfiguration(serverConfiguration);
        this.authorizationService = brokerService.getAuthorizationService();
        this.bundleOwnershipListener = new MQTTNamespaceBundleOwnershipListener(pulsarService.getNamespaceService());
        this.metricsCollector = new MQTTMetricsCollector(serverConfiguration);
        this.metricsProvider = new MQTTMetricsProvider(metricsCollector);
        this.pulsarService.addPrometheusRawMetricsProvider(metricsProvider);
        this.authenticationService = serverConfiguration.isMqttAuthenticationEnabled()
            ? new MQTTAuthenticationService(brokerService,
                serverConfiguration.getMqttAuthenticationMethods(),
                serverConfiguration.isMqttProxyMTlsAuthenticationEnabled()) : null;
        this.connectionManager = new MQTTConnectionManager(pulsarService.getAdvertisedAddress());
        this.subscriptionManager = new MQTTSubscriptionManager();
        if (getServerConfiguration().isMqttProxyEnabled()) {
            this.eventCenter = new DisableEventCenter();
        } else {
            this.eventCenter = new PulsarEventCenterImpl(brokerService,
                    serverConfiguration.getEventCenterCallbackPoolThreadNum());
        }
        this.retainedMessageHandler = new RetainedMessageHandler(this);
        this.qosPublishHandlers = new QosPublishHandlersImpl(this);
        this.willMessageHandler = new WillMessageHandler(this);
    }

    public boolean isSystemTopicEnabled() {
        return eventService != null;
    }

    public void close() {
        this.connectionManager.close();
        this.eventCenter.shutdown();
        if (eventService != null) {
            eventService.close();
        }
        this.willMessageHandler.close();
    }
}
