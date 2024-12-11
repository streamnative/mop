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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Notification;

@Slf4j
public class PulsarEventCenterImpl implements Consumer<Notification>, PulsarEventCenter {
    private final List<PulsarEventListener> listeners;
    private final OrderedExecutor callbackExecutor;

    @SuppressWarnings("UnstableApiUsage")
    public PulsarEventCenterImpl(MetadataStore configurationMetadataStore, int poolThreadNum) {
        this.listeners = new CopyOnWriteArrayList<>();
        this.callbackExecutor = OrderedExecutor.newBuilder()
                .numThreads(poolThreadNum)
                .name("mqtt-notification-workers").build();
        configurationMetadataStore.registerListener(this);
    }


    @Override
    public void register(PulsarEventListener listener) {
        listeners.add(listener);
    }

    @Override
    public void unRegister(PulsarEventListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void shutdown() {
        callbackExecutor.shutdown();
    }

    @Override
    public void accept(Notification notification) {
        if (listeners.isEmpty()) {
            return;
        }
        final String path = notification.getPath();
        final List<PulsarEventListener> matcherListeners = listeners.stream().filter(listeners ->
                listeners.matchPath(path)).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(matcherListeners)) {
            // give the task to another thread to avoid blocking metadata
            callbackExecutor.executeOrdered(path, () -> {
                for (PulsarEventListener listener : matcherListeners) {
                    try {
                        switch (notification.getType()) {
                            case Created:
                                listener.onNodeCreated(path);
                                break;
                            case Deleted:
                                listener.onNodeDeleted(path);
                                break;
                            default:
                                break;
                        }
                    } catch (Throwable ex) {
                        log.warn("notify change {} {} failed.", notification.getType(), notification.getPath(), ex);
                    }
                }
            });
        }
    }
}
