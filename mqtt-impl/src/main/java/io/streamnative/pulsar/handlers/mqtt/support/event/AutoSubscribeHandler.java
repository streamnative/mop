package io.streamnative.pulsar.handlers.mqtt.support.event;


import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.mqtt.TopicFilter;
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
    private final PulsarEventCenter eventCenter;
    private final Map<TopicFilter, List<Consumer<String>>> filterToCallback = new ConcurrentHashMap<>();
    private final Map<TopicFilter, CompletableFuture<Void>> unregisterFutures = new ConcurrentHashMap<>();

    public AutoSubscribeHandler(PulsarEventCenter eventCenter) {
        this.eventCenter = eventCenter;
        eventCenter.register(this);
    }

    public void register(TopicFilter topicFilter, Consumer<String> callback) {
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
