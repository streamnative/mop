/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.mqtt;

import static io.streamnative.pulsar.handlers.mqtt.MqttTimer.getCommonTimer;
import static java.util.Objects.requireNonNull;
import io.netty.util.Timeout;
import io.netty.util.concurrent.EventExecutor;
import io.streamnative.pulsar.handlers.mqtt.support.MQTTConsumer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.api.proto.CommandAck;

@ThreadSafe
public class MqttAckTrackerImpl implements MqttAckTracker {
    private final MQTTConsumer consumer;
    private List<OutstandingPacket> batchContainer;
    private final Object batchContainerMutex;
    private final Semaphore mutex;
    private volatile Timeout makeUpTask;

    public MqttAckTrackerImpl(MQTTConsumer consumer) {
        this.consumer = consumer;
        this.batchContainer = new ArrayList<>();
        this.mutex = new Semaphore(1);
        this.batchContainerMutex = new Object();
    }

    @Override
    public void add(OutstandingPacket outstandingPacket) {
        requireNonNull(outstandingPacket);
        if (!mutex.tryAcquire()) {
            synchronized (batchContainerMutex) {
                // put it in to container
                batchContainer.add(outstandingPacket);
            }
            return;
        }
        if (makeUpTask != null) { // fast-cancel
            makeUpTask.cancel();
        }
        synchronized (batchContainerMutex) {
            batchContainer.add(outstandingPacket);
        }
        final EventExecutor executors = consumer.getCnx().ctx().executor();
        // Using IO Executor to let current thread switching to implement batching
        executors.execute(() -> {
            try {
                flush();
            } finally {
                // introduce a makeup mechanism to help solve the in-flating messages in the batch contianer.
                updateMakeup(getCommonTimer().newTimeout(__ -> flush(), 100, TimeUnit.MILLISECONDS));
                mutex.release();
            }
        });
    }

    @Override
    public void flush() {
        final List<OutstandingPacket> oldBatchContainer;
        synchronized (batchContainerMutex) {
            if (batchContainer == null || batchContainer.isEmpty()) {
                return;
            }
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
        consumer.getSubscription()
                .acknowledgeMessage(positions, CommandAck.AckType.Individual, Collections.emptyMap());
        for (OutstandingPacket packet : oldBatchContainer) {
            packet.getConsumer().getPendingAcks().remove(packet.getLedgerId(), packet.getEntryId());
        }
    }

    private void updateMakeup(Timeout timeout) {
        if (makeUpTask != null) {
            makeUpTask.cancel();
        }
        makeUpTask = timeout;
    }
}
