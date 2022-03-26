package io.streamnative.pulsar.handlers.mqtt.event;

import io.streamnative.pulsar.handlers.mqtt.MQTTServerConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.metadata.api.Notification;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PulsarEventCenterImpl implements Consumer<Notification>, PulsarEventCenter {
    private final List<PulsarEventListener> listeners;
    private final ExecutorService callbackExecutor;

    @SuppressWarnings("UnstableApiUsage")
    public PulsarEventCenterImpl(BrokerService brokerService, MQTTServerConfiguration serverConfiguration) {
        this.listeners = Collections.synchronizedList(new ArrayList<>());
        this.callbackExecutor =
                Executors.newFixedThreadPool(serverConfiguration.getEventCenterCallbackPoolTreadNum());
        brokerService.getPulsar()
                .getConfigurationMetadataStore().registerListener(this);
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
        String path = notification.getPath();
        List<PulsarEventListener> needNotifyListener =
                listeners.stream().filter(listeners -> listeners.matchPath(path)).collect(Collectors.toList());
        callbackExecutor.execute(() -> needNotifyListener.parallelStream()
                .forEach(listener -> callbackExecutor.execute(() -> {
                    switch (notification.getType()) {
                        case Created:
                            listener.onNodeCreated(path);
                            break;
                        case Deleted:
                            listener.onNodeDeleted(path);
                            break;
                    }
                })));
    }
}
