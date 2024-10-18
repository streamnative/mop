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

import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTNoSubscriptionExistedException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class TopicSubscriptionManager {

    private final Map<Topic, Pair<Subscription, Consumer>> topicSubscriptions = Maps.newConcurrentMap();

    public Pair<Subscription, Consumer> putIfAbsent(Topic topic, Subscription subscription, Consumer consumer) {
        return topicSubscriptions.putIfAbsent(topic, Pair.of(subscription, consumer));
    }

    public void removeSubscriptionConsumers() {
        topicSubscriptions.forEach((k, v) -> {
            try {
                removeConsumerIfExist(v.getLeft(), v.getRight());
            } catch (BrokerServiceException ex) {
                log.warn("subscription [{}] remove consumer {} error",
                        v.getLeft(), v.getRight(), ex);
            }
        });
    }

    public CompletableFuture<Void> unsubscribe(Topic topic, boolean cleanSubscription) {
        Pair<Subscription, Consumer> subscriptionConsumerPair = topicSubscriptions.get(topic);
        if (subscriptionConsumerPair == null) {
            return FutureUtil.failedFuture(new MQTTNoSubscriptionExistedException(
                    String.format("Can not found subscription for topic %s when unSubscribe", topic)));
        }
        String subscriberName = subscriptionConsumerPair.getLeft().getName();
        try {
            removeConsumerIfExist(subscriptionConsumerPair.getLeft(), subscriptionConsumerPair.getValue());
        } catch (BrokerServiceException e) {
            log.error("[ Subscription ] Subscription {} Remove consumer fail.", subscriberName, e);
            FutureUtil.failedFuture(e);
        }
        if (cleanSubscription) {
            return subscriptionConsumerPair.getLeft().deleteForcefully()
                    .thenAccept(unused -> topicSubscriptions.remove(topic));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public CompletableFuture<Void> removeSubscriptions() {
        List<CompletableFuture<Void>> futures = topicSubscriptions.keySet()
                .stream()
                .map(topic -> unsubscribe(topic, true))
                .collect(Collectors.toList());
        return FutureUtil.waitForAll(futures);
    }

    private void removeConsumerIfExist(Subscription subscription, Consumer consumer) throws BrokerServiceException {
        if (subscription.getConsumers().contains(consumer)) {
            subscription.removeConsumer(consumer);
        }
    }
}
