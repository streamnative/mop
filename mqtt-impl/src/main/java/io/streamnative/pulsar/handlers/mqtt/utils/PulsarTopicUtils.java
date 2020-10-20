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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentTopic;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Pulsar topic utils.
 */
public class PulsarTopicUtils {

    public static CompletableFuture<Optional<Topic>> getTopicReference(PulsarService pulsarService, String topicName) {
        final TopicName topic = TopicName.get(topicName);
        return pulsarService.getNamespaceService().getBrokerServiceUrlAsync(topic,
                LookupOptions.builder().authoritative(false).loadTopicsInBundle(false).build())
                .thenCompose(lookupOp -> pulsarService.getBrokerService().getTopic(topic.toString(), true));
    }

    public static CompletableFuture<Subscription> getOrCreateSubscription(PulsarService pulsarService,
              String topicName, String subscriptionName) {
        CompletableFuture<Subscription> promise = new CompletableFuture<>();
        getTopicReference(pulsarService, topicName).thenAccept(topicOp -> {
            if (!topicOp.isPresent()) {
                promise.completeExceptionally(new BrokerServiceException.TopicNotFoundException(topicName));
            } else {
                Topic topic = topicOp.get();
                Subscription subscription = topic.getSubscription(subscriptionName);
                if (subscription == null) {
                    topic.createSubscription(subscriptionName,
                        PulsarApi.CommandSubscribe.InitialPosition.Latest, false)
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
                }
            }
        });
        return promise;
    }
}
