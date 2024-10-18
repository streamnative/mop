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
package io.streamnative.pulsar.handlers.mqtt.broker.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Namespace bundler ownership listener.
 */
@Slf4j
public class MQTTNamespaceBundleOwnershipListener implements NamespaceBundleOwnershipListener {

    private final NamespaceService namespaceService;

    private final List<MQTTTopicOwnershipListener> listeners = new ArrayList<>();

    public MQTTNamespaceBundleOwnershipListener(NamespaceService namespaceService) {
        this.namespaceService = namespaceService;
        this.namespaceService.addNamespaceBundleOwnershipListener(this);
    }

    public void addListener(MQTTTopicOwnershipListener listener) {
        listeners.add(listener);
    }

    @Override
    public void onLoad(NamespaceBundle bundle) {
        //
    }

    @Override
    public void unLoad(NamespaceBundle bundle) {
        namespaceService.getFullListOfTopics(bundle.getNamespaceObject())
                .thenApply(topics -> topics.stream()
                        .filter(topic -> bundle.includes(TopicName.get(topic)))
                        .collect(Collectors.toList()))
                .thenAccept(topics -> {
                    log.info("unload namespace bundle : {}, topics : {}", bundle, topics);
                    listeners.forEach(listener -> {
                        if (listener.test(bundle.getNamespaceObject())) {
                            topics.forEach(topic -> listener.unload(TopicName.get(topic)));
                        }
                    });
                }).exceptionally(ex -> {
                    log.error("unload namespace bundle :{} error", bundle, ex);
                    return null;
                });
    }

    @Override
    public boolean test(NamespaceBundle namespaceBundle) {
        return true;
    }
}
