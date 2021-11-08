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
package io.streamnative.pulsar.handlers.mqtt;

import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Proxy connection manager.
 */
@Slf4j
public class MQTTSubscriptionManager {

    private ConcurrentMap<String, List<MqttTopicSubscription>> subscriptions = new ConcurrentHashMap<>(2048);

    public void addSubscriptions(String clientId, List<MqttTopicSubscription> topicSubscriptions) {
        this.subscriptions.computeIfAbsent(clientId, k -> new ArrayList<>()).addAll(topicSubscriptions);
    }

    public List<Pair<String, String>> findMatchTopic(String topic) {
        List<Pair<String, String>> result = new ArrayList<>();
        Set<Map.Entry<String, List<MqttTopicSubscription>>> entries = subscriptions.entrySet();
        for (Map.Entry<String, List<MqttTopicSubscription>> entry : entries) {
            String clientId = entry.getKey();
            List<MqttTopicSubscription> subs = entry.getValue();
            subs.forEach(sub -> {
                if (new TopicFilterImpl(sub.topicName()).test(topic)) {
                    result.add(Pair.of(clientId, sub.topicName()));
                }
            });
        }
        return result;
    }

    public void removeSubscription(String clientId) {
        subscriptions.remove(clientId);
    }
}
