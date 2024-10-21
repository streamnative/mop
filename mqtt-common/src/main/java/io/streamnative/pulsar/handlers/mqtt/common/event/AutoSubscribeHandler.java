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
package io.streamnative.pulsar.handlers.mqtt.common.event;


import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.mqtt.common.TopicFilter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;

@Slf4j
public class AutoSubscribeHandler implements PulsarTopicChangeListener {

    private volatile boolean registered = false;
    private final PulsarEventCenter eventCenter;
    private final Map<TopicFilter, List<Consumer<String>>> filterToCallback = new ConcurrentHashMap<>();
    private final Map<TopicFilter, CompletableFuture<Void>> unregisterFutures = new ConcurrentHashMap<>();

    public AutoSubscribeHandler(PulsarEventCenter eventCenter) {
        this.eventCenter = eventCenter;
    }

    public void register(TopicFilter topicFilter, Consumer<String> callback) {
        if (!registered) {
            registerToEventCenter();
        }
        filterToCallback.compute(topicFilter, (k, consumers) -> {
           if (consumers == null) {
               return Lists.newArrayList(callback);
           } else {
               consumers.add(callback);
               return consumers;
           }
       });
        CompletableFuture<Void> unregisterFuture = new CompletableFuture<>();
        unregisterFutures.put(topicFilter, unregisterFuture);
        unregisterFuture.whenComplete((__ , ex) -> {
            filterToCallback.compute(topicFilter, (k, v) -> {
               if (!CollectionUtils.isEmpty(v)) {
                   v.remove(callback);
               }
               return v;
           });
        });
    }

    private synchronized void registerToEventCenter() {
        if (!registered) {
            eventCenter.register(this);
            registered = true;
        }
    }

    public void unregister(TopicFilter filter) {
        CompletableFuture<Void> future = unregisterFutures.remove(filter);
        if (future == null) {
            return;
        }
        future.complete(null);
    }

    @Override
    public void onTopicLoad(TopicName topicName) {
        List<Consumer<String>> needNotifyCallback = filterToCallback.keySet()
                .stream()
                .filter(filter -> filter.test(Codec.decode(topicName.getLocalName())))
                .map(filterToCallback::get)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(needNotifyCallback)) {
            return;
        }
        for (Consumer<String> consumer : needNotifyCallback) {
            try {
                consumer.accept(topicName.toString());
            } catch (Throwable ex) {
                log.error("Failed to auto subscribe topic {}", topicName.toString());
            }
        }
    }

    @Override
    public void onTopicUnload(TopicName topicName) {
        // NO-OP
    }

    public void close() {
        filterToCallback.clear();
        unregisterFutures.clear();
        eventCenter.unRegister(this);
    }


}
