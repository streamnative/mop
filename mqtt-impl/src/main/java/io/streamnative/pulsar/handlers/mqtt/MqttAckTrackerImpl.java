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

import static java.util.Objects.requireNonNull;
import io.netty.util.concurrent.EventExecutor;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTConsumer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.CommandAck;

public class MqttAckTrackerImpl implements MqttAckTracker {
    private List<OutstandingPacket> batchContainer;
    private final Object batchContainerMutex;
    private final Semaphore mutex;

    public MqttAckTrackerImpl() {
        this.batchContainer = new ArrayList<>();
        this.mutex = new Semaphore(1);
        this.batchContainerMutex = new Object();
    }

    @Override
    public void add(OutstandingPacket outstandingPacket) {
        requireNonNull(outstandingPacket);
        final MQTTConsumer consumer = outstandingPacket.getConsumer();
        final Subscription sub = outstandingPacket.getConsumer().getSubscription();
        if (!mutex.tryAcquire()) {
            synchronized (batchContainerMutex) {
                // put it in to container
                batchContainer.add(outstandingPacket);
            }
            return;
        }
        batchContainer.add(outstandingPacket);
        final EventExecutor executors = consumer.getCnx().ctx().executor();
        // Using IO Executor to let current thread switching to implement batching
        executors.execute(() -> {
            try {
                final List<OutstandingPacket> oldBatchContainer;
                synchronized (batchContainerMutex) {
                    oldBatchContainer = batchContainer;
                    batchContainer = new ArrayList<>();
                }
                final List<Position> positions = oldBatchContainer.stream().map(packet -> {
                    if (packet.isBatch()) {
                        long[] ackSets = new long[packet.getBatchSize()];
                        for (int i = 0; i < packet.getBatchSize(); i++) {
                            ackSets[i] = packet.getBatchIndex() == i ? 0 : 1;
                        }
                        return PositionImpl.get(packet.getLedgerId(), packet.getEntryId(), ackSets);
                    } else {
                        return PositionImpl.get(packet.getLedgerId(), packet.getEntryId());
                    }
                }).collect(Collectors.toList());
                sub.acknowledgeMessage(positions, CommandAck.AckType.Individual, Collections.emptyMap());
                for (OutstandingPacket packet : oldBatchContainer) {
                    packet.getConsumer().getPendingAcks().remove(packet.getLedgerId(), packet.getEntryId());
                }
            } finally {
                mutex.release();
            }
        });
    }
}
