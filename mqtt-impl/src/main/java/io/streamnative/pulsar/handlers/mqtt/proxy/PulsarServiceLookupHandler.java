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

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import io.streamnative.pulsar.handlers.mqtt.utils.ConfigurationUtils;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.BackoffBuilder;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
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
    private final PulsarService pulsarService;
    private final MQTTProxyConfiguration proxyConfig;
    private final ExecutorProvider executorProvider;

    public PulsarServiceLookupHandler(PulsarService pulsarService, MQTTProxyConfiguration proxyConfig) {
        this.pulsarService = pulsarService;
        this.proxyConfig = proxyConfig;
        this.executorProvider = new ExecutorProvider(proxyConfig.getLookupThreadPoolNum(), "mop-lookup-thread");
        this.localBrokerDataCache = pulsarService
                .getLocalMetadataStore().getMetadataCache(LocalBrokerData.class);
        this.pulsarClient = getClient(proxyConfig);
    }

    private void findBroker(TopicName topicName,
                            Backoff backoff,
                            AtomicLong remainingTime,
                            CompletableFuture<InetSocketAddress> future) {
        pulsarClient.getLookup().getBroker(topicName)
                .thenCompose(lookupResult -> {
                        final var lookupPair = Pair.of(lookupResult.getLogicalAddress(), lookupResult.getPhysicalAddress());
                        return localBrokerDataCache.getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT).thenCompose(brokers -> {
                            // Get all broker data by metadata
                            List<CompletableFuture<Optional<LocalBrokerData>>> brokerDataFutures =
                                    Collections.unmodifiableList(brokers.stream()
                                            .map(brokerPath -> String.format("%s/%s",
                                                    LoadManager.LOADBALANCE_BROKERS_ROOT, brokerPath))
                                            .map(localBrokerDataCache::get)
                                            .collect(Collectors.toList()));
                            return FutureUtil.waitForAll(brokerDataFutures)
                                    .thenCompose(__ -> {
                                        // Find specific broker same to lookup
                                        Optional<LocalBrokerData> specificBrokerData =
                                                brokerDataFutures.stream().map(CompletableFuture::join)
                                                        .filter(brokerData -> brokerData.isPresent()
                                                                && isLookupMQTTBroker(lookupPair, brokerData.get()))
                                                        .map(Optional::get)
                                                        .findAny();
                                        if (specificBrokerData.isEmpty()) {
                                            return FutureUtil.failedFuture(new BrokerServiceException(
                                                    "The broker does not enabled the mqtt protocol handler."));
                                        }
                                        // Get MQTT protocol listeners
                                        Optional<String> protocol = specificBrokerData.get()
                                                .getProtocol(protocolHandlerName);
                                        assert protocol.isPresent();
                                        String mqttBrokerUrls = protocol.get();
                                        String[] brokerUrls = mqttBrokerUrls.split(ConfigurationUtils.LISTENER_DEL);
                                        // Get url random
                                        Optional<String> brokerUrl = Arrays.stream(brokerUrls)
                                                .filter(url -> url.startsWith(ConfigurationUtils.PLAINTEXT_PREFIX))
                                                .findAny();
                                        if (!brokerUrl.isPresent()) {
                                            return FutureUtil.failedFuture(new BrokerServiceException(
                                                    "The broker does not enabled the mqtt protocol handler."));
                                        }
                                        String[] splits = brokerUrl.get().split(ConfigurationUtils.COLON);
                                        String port = splits[splits.length - 1];
                                        int mqttBrokerPort = Integer.parseInt(port);
                                        return CompletableFuture.completedFuture(new InetSocketAddress(lookupPair
                                                .getLeft().getHostName(), mqttBrokerPort));
                                    });
                        });
                })
                .thenAccept(future::complete)
                .exceptionally(e -> {
                    long nextDelay = Math.min(backoff.next(), remainingTime.get());
                    // skip retry scheduler when `TooManyRequestsException`
                    boolean isLookupThrottling = !PulsarClientException.isRetriableError(e.getCause())
                            || e.getCause() instanceof PulsarClientException.TooManyRequestsException
                            || e.getCause() instanceof PulsarClientException.AuthenticationException;
                    if (nextDelay <= 0 || isLookupThrottling) {
                        future.completeExceptionally(e);
                        return null;
                    }

                    ((ScheduledExecutorService) executorProvider.getExecutor()).schedule(() -> {
                        log.warn("[topic: {}] Could not get topic lookup result -- Will try again in {} ms",
                                topicName, nextDelay);
                        remainingTime.addAndGet(-nextDelay);
                        findBroker(topicName, backoff, remainingTime, future);
                    }, nextDelay, TimeUnit.MILLISECONDS);
                    return null;
                });
    }

    @Override
    public CompletableFuture<InetSocketAddress> findBroker(TopicName topicName) {
        CompletableFuture<InetSocketAddress> lookupResult = new CompletableFuture<>();
        AtomicLong opTimeoutMs = new AtomicLong(proxyConfig.getLookupOperationTimeoutMs());
        Backoff backoff = new BackoffBuilder()
                .setInitialTime(100, TimeUnit.MILLISECONDS)
                .setMandatoryStop(opTimeoutMs.get() * 2, TimeUnit.MILLISECONDS)
                .setMax(proxyConfig.getMaxLookupIntervalMs(), TimeUnit.MILLISECONDS)
                .create();

        findBroker(topicName, backoff, opTimeoutMs, lookupResult);
        return lookupResult;
    }

    private boolean isLookupMQTTBroker(Pair<InetSocketAddress, InetSocketAddress> pair,
                                       LocalBrokerData localBrokerData) {

        String plain = String.format("pulsar://%s:%s", pair.getLeft().getHostName(), pair.getLeft().getPort());
        String ssl = String.format("pulsar+ssl://%s:%s", pair.getLeft().getHostName(), pair.getLeft().getPort());
        return localBrokerData.getProtocol(protocolHandlerName).isPresent()
                && (localBrokerData.getPulsarServiceUrl().equals(plain)
                    || localBrokerData.getPulsarServiceUrlTls().equals(ssl));
    }

    @Override
    public void close() {
        try {
            pulsarClient.close();
            executorProvider.shutdownNow();
        } catch (PulsarClientException ignore) {
        }
    }

    private PulsarClientImpl getClient(MQTTProxyConfiguration proxyConfig) {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(proxyConfig.isTlsEnabled()
                ? pulsarService.getBrokerServiceUrlTls() : pulsarService.getBrokerServiceUrl());
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
}
