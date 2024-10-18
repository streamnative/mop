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
package io.streamnative.pulsar.handlers.mqtt.utils;

import static io.streamnative.pulsar.handlers.mqtt.utils.MqttUtils.isRegexFilter;
import com.google.common.base.Splitter;
import io.streamnative.pulsar.handlers.mqtt.common.TopicFilter;
import io.streamnative.pulsar.handlers.mqtt.common.TopicFilterImpl;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.util.FutureUtil;


/**
 * Pulsar topic utils.
 */
public class PulsarTopicUtils {

    public static final String PERSISTENT_DOMAIN = TopicDomain.persistent.value() + "://";
    public static final String NON_PERSISTENT_DOMAIN = TopicDomain.non_persistent.value() + "://";

    public static CompletableFuture<Optional<Topic>> getTopicReference(PulsarService pulsarService, String topicName,
                                                                       String defaultTenant, String defaultNamespace,
                                                                       boolean encodeTopicName,
                                                                       String defaultTopicDomain) {
        return getTopicName(topicName, defaultTenant, defaultNamespace, encodeTopicName, defaultTopicDomain)
                .thenCompose(topic -> pulsarService.getPulsarResources().getNamespaceResources()
                        .getPoliciesAsync(topic.getNamespaceObject())
                        .thenApply(policies -> {
                            if (!policies.isPresent()) {
                                return pulsarService.getConfig().isAllowAutoTopicCreation();
                            }
                            AutoTopicCreationOverride autoTopicCreationOverride =
                                    policies.get().autoTopicCreationOverride;
                            if (autoTopicCreationOverride == null) {
                                return pulsarService.getConfig().isAllowAutoTopicCreation();
                            } else {
                                return autoTopicCreationOverride.isAllowAutoTopicCreation();
                            }
                        }).thenCompose(isAllowTopicCreation ->
                                getTopicReference(pulsarService, topic, isAllowTopicCreation)));
    }

    public static CompletableFuture<Optional<Topic>> getTopicReference(PulsarService pulsarService, String topicName,
                                                                       String defaultTenant, String defaultNamespace,
                                                                       boolean encodeTopicName,
                                                                       String defaultTopicDomain,
                                                                       Boolean createIfMissing) {
        return getTopicName(topicName, defaultTenant, defaultNamespace, encodeTopicName, defaultTopicDomain)
                .thenCompose(topic -> getTopicReference(pulsarService, topic, createIfMissing));
    }

    public static CompletableFuture<Optional<Topic>> getTopicReference(PulsarService pulsarService, TopicName topic,
                                                                       Boolean createIfMissing) {
        return pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topic,
                        LookupOptions.builder().authoritative(false).loadTopicsInBundle(false).build())
                .thenCompose(lookupOp -> pulsarService.getBrokerService().getTopic(topic.toString(), createIfMissing));
    }

    public static CompletableFuture<Subscription> getOrCreateSubscription(PulsarService pulsarService,
                                                                          String topicName, String subscriptionName,
                                                                          String defaultTenant, String defaultNamespace,
                                                                          String defaultTopicDomain,
                                                                          CommandSubscribe.InitialPosition position) {
        CompletableFuture<Subscription> promise = new CompletableFuture<>();
        getTopicReference(pulsarService, topicName, defaultTenant, defaultNamespace, false,
                defaultTopicDomain).thenAccept(topicOp -> {
            if (!topicOp.isPresent()) {
                promise.completeExceptionally(new BrokerServiceException.TopicNotFoundException(topicName));
            } else {
                Topic topic = topicOp.get();
                Subscription subscription = topic.getSubscription(subscriptionName);
                if (subscription == null) {
                    topic.createSubscription(subscriptionName, position, false, new HashMap<>())
                            .thenAccept(sub -> {
                                if (topic instanceof NonPersistentTopic) {
                                    ((NonPersistentTopic) topic).getSubscriptions().put(subscriptionName,
                                            (NonPersistentSubscription) sub);
                                }
                                promise.complete(sub);
                            })
                            .exceptionally(e -> {
                                promise.completeExceptionally(e);
                                return null;
                            });
                } else {
                    promise.complete(subscription);
                }
            }
        }).exceptionally(ex -> {
            promise.completeExceptionally(ex);
            return null;
        });
        return promise;
    }

    public static String getEncodedPulsarTopicName(String mqttTopicName, String defaultTenant,
           String defaultNamespace, TopicDomain topicDomain) {
        return getPulsarTopicName(mqttTopicName, defaultTenant, defaultNamespace, true, topicDomain);
    }

    public static String getPulsarTopicName(String mqttTopicName, String defaultTenant, String defaultNamespace,
            boolean urlEncoded, TopicDomain topicDomain) {
        if (mqttTopicName.startsWith(PERSISTENT_DOMAIN)
                || mqttTopicName.startsWith(NON_PERSISTENT_DOMAIN)) {
            List<String> parts = Splitter.on("://").limit(2).splitToList(mqttTopicName);
            if (parts.size() < 2) {
                throw new IllegalArgumentException("Invalid topic name: " + mqttTopicName);
            }
            String domain = parts.get(0);
            String rest = parts.get(1);
            parts = Splitter.on("/").limit(3).splitToList(rest);
            if (parts.size() < 3) {
                throw new IllegalArgumentException("Invalid topic name: " + mqttTopicName);
            }
            String tenant = parts.get(0);
            String namespace = parts.get(1);
            String localName = parts.get(2);
            return TopicName.get(domain, tenant, namespace,
                    urlEncoded ? URLEncoder.encode(localName) : localName).toString();
        } else {
            return TopicName.get(topicDomain.value(), defaultTenant, defaultNamespace,
                    URLEncoder.encode(mqttTopicName)).toString();
        }
    }

    public static Pair<TopicDomain, NamespaceName> getTopicDomainAndNamespaceFromTopicFilter(String mqttTopicFilter,
            String defaultTenant, String defaultNamespace, String defaultTopicDomain) {
        if (mqttTopicFilter.startsWith(PERSISTENT_DOMAIN)
                || mqttTopicFilter.startsWith(NON_PERSISTENT_DOMAIN)) {
            List<String> parts = Splitter.on("://").limit(2).splitToList(mqttTopicFilter);
            if (parts.size() < 2) {
                throw new IllegalArgumentException("Invalid topic filter: " + mqttTopicFilter);
            }
            String domain = parts.get(0);
            String rest = parts.get(1);
            parts = Splitter.on("/").limit(3).splitToList(rest);
            if (parts.size() < 3) {
                throw new IllegalArgumentException("Invalid topic filter: " + mqttTopicFilter);
            }
            String tenant = parts.get(0);
            String namespace = parts.get(1);
            return Pair.of(TopicDomain.getEnum(domain), NamespaceName.get(tenant, namespace));
        } else {
            return Pair.of(TopicDomain.getEnum(defaultTopicDomain), NamespaceName.get(defaultTenant, defaultNamespace));
        }
    }

    public static TopicFilter getTopicFilter(String mqttTopicFilter) {
        if (mqttTopicFilter.startsWith(PERSISTENT_DOMAIN)
                || mqttTopicFilter.startsWith(NON_PERSISTENT_DOMAIN)) {
            List<String> parts = Splitter.on("://").limit(2).splitToList(mqttTopicFilter);
            if (parts.size() < 2) {
                throw new IllegalArgumentException("Invalid topic filter: " + mqttTopicFilter);
            }
            String rest = parts.get(1);
            parts = Splitter.on("/").limit(3).splitToList(rest);
            if (parts.size() < 3) {
                throw new IllegalArgumentException("Invalid topic filter: " + mqttTopicFilter);
            }
            String localName = parts.get(2);
            return new TopicFilterImpl(localName);
        } else {
            return new TopicFilterImpl(mqttTopicFilter);
        }
    }

    public static CompletableFuture<List<String>> asyncGetTopicListFromTopicSubscription(String topicFilter,
         String defaultTenant, String defaultNamespace, PulsarService pulsarService, String defaultTopicDomain) {
        if (isRegexFilter(topicFilter)) {
            TopicFilter filter = PulsarTopicUtils.getTopicFilter(topicFilter);
            Pair<TopicDomain, NamespaceName> domainNamespacePair =
                    PulsarTopicUtils
                            .getTopicDomainAndNamespaceFromTopicFilter(topicFilter, defaultTenant,
                            defaultNamespace, defaultTopicDomain);
            return pulsarService.getNamespaceService().getListOfTopics(
                    domainNamespacePair.getRight(), domainNamespacePair.getLeft() == TopicDomain.persistent
                            ? CommandGetTopicsOfNamespace.Mode.PERSISTENT
                            : CommandGetTopicsOfNamespace.Mode.NON_PERSISTENT).thenCompose(topics ->
                    CompletableFuture.completedFuture(topics.stream().filter(t ->
                                    filter.test(URLDecoder.decode(TopicName.get(t).getLocalName())))
                            .collect(Collectors.toList())));
        } else {
            return CompletableFuture.completedFuture(Collections.singletonList(
                    PulsarTopicUtils.getEncodedPulsarTopicName(topicFilter, defaultTenant, defaultNamespace,
                            TopicDomain.getEnum(defaultTopicDomain))));
        }
    }

    public static String getToConsumerTopicName(String subTopicFilter, String pulsarTopicName) {
        if (subTopicFilter.startsWith(TopicDomain.persistent.value())
                || subTopicFilter.startsWith(TopicDomain.non_persistent.value())) {
            TopicName topicName = TopicName.get(pulsarTopicName);
            return topicName.getDomain().toString() + "://"
                    + NamespaceName.get(topicName.getNamespace()).toString() + "/"
                    + URLDecoder.decode(topicName.getLocalName());
        } else {
            return URLDecoder.decode(TopicName.get(pulsarTopicName).getLocalName());
        }
    }

    public static CompletableFuture<TopicName> getTopicName(String topicName, String defaultTenant,
                                                            String defaultNamespace, boolean encodeTopicName,
                                                            String defaultTopicDomain) {
        try {
            return CompletableFuture.completedFuture(
                    TopicName.get(
                            getPulsarTopicName(topicName, defaultTenant, defaultNamespace, encodeTopicName,
                                TopicDomain.getEnum(defaultTopicDomain))));
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
    }

    public static boolean isDefaultDomainAndNs(TopicName topicName,
                                               String defaultTopicDomain,
                                               String defaultTenant,
                                               String defaultNamespace) {
        return Objects.equals(topicName.getDomain().value(), defaultTopicDomain)
                && Objects.equals(topicName.getNamespace(), defaultTenant + "/" + defaultNamespace);
    }
}
