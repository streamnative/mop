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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Subscription manager.
 */
@Slf4j
public class MQTTSubscriptionManager {

    private ConcurrentMap<String, List<MqttTopicSubscription>> subscriptions = new ConcurrentHashMap<>(2048);

    public boolean addSubscriptions(String clientId, List<MqttTopicSubscription> topicSubscriptions) {
        boolean duplicated = false;
        List<MqttTopicSubscription> preSubscriptions = this.subscriptions.putIfAbsent(clientId, topicSubscriptions);
        if (preSubscriptions != null) {
            synchronized (clientId.intern()) {
                List<String> preTopicNameList = preSubscriptions.stream().map(MqttTopicSubscription::topicName)
                        .collect(Collectors.toList());
                List<String> curTopicNameList = topicSubscriptions.stream().map(MqttTopicSubscription::topicName)
                        .collect(Collectors.toList());
                Collection<String> interTopicNameList = CollectionUtils
                        .intersection(preTopicNameList, curTopicNameList);
                if (interTopicNameList.isEmpty()) {
                    preSubscriptions.addAll(topicSubscriptions);
                } else {
                    log.error("duplicate subscribe topic filter : {}", interTopicNameList);
                    duplicated = true;
                }
            }
        }
        return duplicated;
    }

    /**
     *  Find the matched topic from the subscriptions.
     * @param topic
     * @return Pair with clientId, topicName.
     */
    public List<Pair<String, String>> findMatchedTopic(String topic) {
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
