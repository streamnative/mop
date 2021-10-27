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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

/**
 * The proxy start with broker, use this lookup handler to find broker.
 */
@Slf4j
public class PulsarServiceLookupHandler implements LookupHandler {

    private final String protocolHandlerName = "mqtt";

    private final PulsarClientImpl pulsarClient;
    private final MetadataCache<LocalBrokerData> localBrokerDataCache;

    public PulsarServiceLookupHandler(PulsarService pulsarService, MQTTProxyConfiguration proxyConfig)
            throws MQTTProxyException {
        this.localBrokerDataCache = pulsarService.getLocalMetadataStore().getMetadataCache(LocalBrokerData.class);
        try {
            this.pulsarClient = new PulsarClientImpl(createClientConfiguration(proxyConfig));
        } catch (PulsarClientException e) {
            throw new MQTTProxyException(e);
        }
    }

    @Override
    public CompletableFuture<InetSocketAddress> findBroker(TopicName topicName) {
        CompletableFuture<InetSocketAddress> lookupResult = new CompletableFuture<>();
        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> lookup =
                pulsarClient.getLookup().getBroker(topicName);

        lookup.whenComplete((pair, throwable) -> {
            if (null != throwable) {
                log.error("Failed to perform lookup request for topic {}", topicName, throwable);
                lookupResult.completeExceptionally(throwable);
            } else {
                localBrokerDataCache.getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT).thenAccept(list -> {
                    List<CompletableFuture<Optional<LocalBrokerData>>> futures = new ArrayList<>(list.size());
                    for (String webServiceUrl : list) {
                        final String path = String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, webServiceUrl);
                        futures.add(localBrokerDataCache.get(path));
                    }
                    FutureUtil.waitForAll(futures).thenAccept(__ -> {
                        boolean foundOwner = false;
                        for (CompletableFuture<Optional<LocalBrokerData>> future : futures) {
                            try {
                                Optional<LocalBrokerData> op = future.get();
                                if (op.isPresent()
                                    && op.get().getPulsarServiceUrl().equals("pulsar://" + pair.getLeft().toString())
                                    && op.get().getProtocol(protocolHandlerName).isPresent()) {
                                    String mqttBrokerUrl = op.get().getProtocol(protocolHandlerName).get();
                                    String[] splits = mqttBrokerUrl.split(":");
                                    String port = splits[splits.length - 1];
                                    int mqttBrokerPort = Integer.parseInt(port);
                                    lookupResult.complete(InetSocketAddress.createUnresolved(
                                            pair.getLeft().getHostName(), mqttBrokerPort));
                                    foundOwner = true;
                                    break;
                                }
                            } catch (Exception e) {
                                lookupResult.completeExceptionally(e);
                            }
                        }
                        if (!foundOwner) {
                            lookupResult.completeExceptionally(
                                    new BrokerServiceException(
                                            "The broker does not enabled the mqtt protocol handler."));
                        }
                    }).exceptionally(e -> {
                        lookupResult.completeExceptionally(e);
                        return null;
                    });
                });
            }
        });
        return lookupResult;
    }

    @Override
    public void close() {
        try {
            pulsarClient.close();
        } catch (PulsarClientException ignore) {
        }
    }

    private ClientConfigurationData createClientConfiguration(MQTTProxyConfiguration proxyConfig)
            throws PulsarClientException.UnsupportedAuthenticationException {
        ClientConfigurationData clientConf = new ClientConfigurationData();
        clientConf.setServiceUrl(proxyConfig.getBrokerServiceURL());
        if (proxyConfig.getBrokerClientAuthenticationPlugin() != null) {
            clientConf.setAuthentication(AuthenticationFactory.create(
                    proxyConfig.getBrokerClientAuthenticationPlugin(),
                    proxyConfig.getBrokerClientAuthenticationParameters())
            );
        }
        return clientConf;
    }
}
