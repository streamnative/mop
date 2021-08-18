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
package io.streamnative.pulsar.handlers.mqtt.support;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacketContainer;
import io.streamnative.pulsar.handlers.mqtt.PacketIdGenerator;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarMessageConverter;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;

/**
 * MQTT consumer.
 */
public class MQTTConsumer extends Consumer {

    private final String pulsarTopicName;
    private final String mqttTopicName;
    private final MQTTServerCnx cnx;
    private final MqttQoS qos;
    private final PacketIdGenerator packetIdGenerator;
    private final OutstandingPacketContainer outstandingPacketContainer;
    private static final AtomicIntegerFieldUpdater<MQTTConsumer> MESSAGE_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(MQTTConsumer.class, "availablePermits");
    private volatile int availablePermits;

    private static final AtomicIntegerFieldUpdater<MQTTConsumer> ADD_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(MQTTConsumer.class, "addPermits");
    private volatile int addPermits = 0;
    private final int maxPermits = 1000;

    public MQTTConsumer(Subscription subscription, String mqttTopicName, String pulsarTopicName, String consumerName,
                        MQTTServerCnx cnx, MqttQoS qos, PacketIdGenerator packetIdGenerator,
                        OutstandingPacketContainer outstandingPacketContainer) {
        super(subscription, CommandSubscribe.SubType.Shared, pulsarTopicName, 0, 0, consumerName, 0, cnx,

                "", null, false, CommandSubscribe.InitialPosition.Latest, null, MessageId.latest);
        this.pulsarTopicName = pulsarTopicName;
        this.mqttTopicName = mqttTopicName;
        this.cnx = cnx;
        this.qos = qos;
        this.packetIdGenerator = packetIdGenerator;
        this.outstandingPacketContainer = outstandingPacketContainer;
    }

    @Override
    public ChannelPromise sendMessages(List<Entry> entries, EntryBatchSizes batchSizes,
           EntryBatchIndexesAcks batchIndexesAcks, int totalMessages, long totalBytes, long totalChunkedMessages,
           RedeliveryTracker redeliveryTracker) {
        ChannelPromise promise = cnx.ctx().newPromise();
        MESSAGE_PERMITS_UPDATER.addAndGet(this, -totalMessages);
        for (Entry entry : entries) {
            int packetId = 0;
            if (MqttQoS.AT_MOST_ONCE != qos) {
                packetId = packetIdGenerator.nextPackedId();
                outstandingPacketContainer.add(new OutstandingPacket(this, packetId, entry.getLedgerId(),
                        entry.getEntryId()));
            }
            List<MqttPublishMessage> messages = PulsarMessageConverter.toMqttMessages(mqttTopicName, entry,
                    packetId, qos);
            for (MqttPublishMessage msg : messages) {
                cnx.ctx().channel().write(msg);
            }
        }
        if (MqttQoS.AT_MOST_ONCE == qos) {
            incrementPermits(totalMessages);
            if (entries.size() > 0) {
                getSubscription().acknowledgeMessage(
                    Collections.singletonList(entries.get(entries.size() - 1).getPosition()),
                    CommandAck.AckType.Cumulative, Collections.emptyMap());
            }
        }
        cnx.ctx().channel().writeAndFlush(Unpooled.EMPTY_BUFFER, promise);
        return promise;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int getAvailablePermits() {
        return availablePermits;
    }

    public void incrementPermits() {
        incrementPermits(1);
    }

    public void incrementPermits(int permits) {
        int var = ADD_PERMITS_UPDATER.addAndGet(this, permits);
        if (var > maxPermits / 2) {
            MESSAGE_PERMITS_UPDATER.addAndGet(this, var);
            this.getSubscription().consumerFlow(this, availablePermits);
            ADD_PERMITS_UPDATER.set(this, 0);
        }
    }

    public void addAllPermits() {
        this.availablePermits = maxPermits;
        this.getSubscription().consumerFlow(this, availablePermits);
    }

    @Override
    public boolean isBlocked() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), pulsarTopicName, cnx);
    }
}
