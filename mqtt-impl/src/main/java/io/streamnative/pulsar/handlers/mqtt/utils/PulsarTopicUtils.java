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

import com.google.common.base.Splitter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import io.streamnative.pulsar.handlers.mqtt.TopicFilter;
import io.streamnative.pulsar.handlers.mqtt.TopicFilterImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Pulsar topic utils.
 */
public class PulsarTopicUtils {

    public static final String UTF8 = "UTF-8";
    public static final String PERSISTENT_DOMAIN = TopicDomain.persistent.value() + "://";
    public static final String NON_PERSISTENT_DOMAIN = TopicDomain.non_persistent.value() + "://";

    public static CompletableFuture<Optional<Topic>> getTopicReference(PulsarService pulsarService, String topicName,
           String defaultTenant, String defaultNamespace) {
        final TopicName topic;
        try {
            topic = TopicName.get(getPulsarTopicName(topicName, defaultTenant, defaultNamespace));
        } catch (Exception e) {
            return FutureUtil.failedFuture(e);
        }
        return pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topic,
                LookupOptions.builder().authoritative(false).loadTopicsInBundle(false).build())
                .thenCompose(lookupOp -> pulsarService.getBrokerService().getTopic(topic.toString(), true));
    }

    public static CompletableFuture<Subscription> getOrCreateSubscription(PulsarService pulsarService,
              String topicName, String subscriptionName, String defaultTenant, String defaultNamespace) {
        CompletableFuture<Subscription> promise = new CompletableFuture<>();
        getTopicReference(pulsarService, topicName, defaultTenant, defaultNamespace).thenAccept(topicOp -> {
            if (!topicOp.isPresent()) {
                promise.completeExceptionally(new BrokerServiceException.TopicNotFoundException(topicName));
            } else {
                Topic topic = topicOp.get();
                Subscription subscription = topic.getSubscription(subscriptionName);
                if (subscription == null) {
                    topic.createSubscription(subscriptionName,
                        CommandSubscribe.InitialPosition.Latest, false)
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

    public static String getPulsarTopicName(String mqttTopicName, String defaultTenant, String defaultNamespace)
            throws UnsupportedEncodingException {
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
            return TopicName.get(domain, tenant, namespace, URLEncoder.encode(localName, UTF8)).toString();
        } else {
            return TopicName.get(TopicDomain.persistent.value(), defaultTenant, defaultNamespace,
                    URLEncoder.encode(mqttTopicName, UTF8)).toString();
        }
    }

    public static Pair<TopicDomain,NamespaceName> getTopicDomainAndNamespaceFromTopicFilter(String mqttTopicFilter,
            String defaultTenant, String defaultNamespace) {
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
            return Pair.of(TopicDomain.persistent, NamespaceName.get(defaultTenant, defaultNamespace));
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
}
