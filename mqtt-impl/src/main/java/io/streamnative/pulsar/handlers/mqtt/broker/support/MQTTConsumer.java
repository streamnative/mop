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
package io.streamnative.pulsar.handlers.mqtt.broker.support;

import static io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils.getMessageExpiryInterval;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Future;
import io.streamnative.pulsar.handlers.mqtt.common.Connection;
import io.streamnative.pulsar.handlers.mqtt.adapter.MqttAdapterMessage;
import io.streamnative.pulsar.handlers.mqtt.broker.metric.MQTTMetricsCollector;
import io.streamnative.pulsar.handlers.mqtt.restrictions.ClientRestrictions;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarTopicUtils;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.protocol.Commands;

/**
 * MQTT consumer.
 */
@Slf4j
public class MQTTConsumer extends Consumer {

    private final String pulsarTopicName;
    private final String mqttTopicName;
    private final MQTTServerCnx cnx;
    private final MqttQoS qos;
    private final PacketIdGenerator packetIdGenerator;
    private final OutstandingPacketContainer outstandingPacketContainer;
    private final MQTTMetricsCollector metricsCollector;
    private static final AtomicIntegerFieldUpdater<MQTTConsumer> MESSAGE_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(MQTTConsumer.class, "availablePermits");
    private volatile int availablePermits;

    private static final AtomicIntegerFieldUpdater<MQTTConsumer> ADD_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(MQTTConsumer.class, "addPermits");
    private volatile int addPermits = 0;
    private final ClientRestrictions clientRestrictions;
    @Getter
    private final Connection connection;

    public MQTTConsumer(Subscription subscription, String mqttTopicName, String pulsarTopicName, Connection connection,
                        MQTTServerCnx cnx, MqttQoS qos, PacketIdGenerator packetIdGenerator,
                        OutstandingPacketContainer outstandingPacketContainer, MQTTMetricsCollector metricsCollector) {
        super(subscription, CommandSubscribe.SubType.Shared, pulsarTopicName, 0, 0,
                connection.getClientId(), true, cnx, "", null, false,
                null, MessageId.latest, Commands.DEFAULT_CONSUMER_EPOCH);
        this.pulsarTopicName = pulsarTopicName;
        this.mqttTopicName = mqttTopicName;
        this.cnx = cnx;
        this.qos = qos;
        this.packetIdGenerator = packetIdGenerator;
        this.outstandingPacketContainer = outstandingPacketContainer;
        this.metricsCollector = metricsCollector;
        this.clientRestrictions = connection.getClientRestrictions();
        this.connection = connection;
    }

    @Override
    public Future<Void> sendMessages(List<? extends Entry> entries, EntryBatchSizes batchSizes,
                                     EntryBatchIndexesAcks batchIndexesAcks, int totalMessages, long totalBytes,
                                     long totalChunkedMessages, RedeliveryTracker redeliveryTracker) {
        ChannelPromise promise = cnx.ctx().newPromise();
        MESSAGE_PERMITS_UPDATER.addAndGet(this, -totalMessages);
        for (Entry entry : entries) {
            String toConsumerTopicName = PulsarTopicUtils.getToConsumerTopicName(mqttTopicName, pulsarTopicName);
            List<MqttPublishMessage> messages = PulsarMessageConverter.toMqttMessages(toConsumerTopicName, entry,
                    packetIdGenerator, qos);
            if (MqttQoS.AT_MOST_ONCE != qos) {
                final boolean isBatch = messages.size() > 1;
                if (isBatch) {
                    for (int i = 0; i < messages.size(); i++) {
                        int packetId = messages.get(i).variableHeader().packetId();
                        OutstandingPacket outstandingPacket = new OutstandingPacket(this, packetId, entry.getLedgerId(),
                                        entry.getEntryId(), i, messages.size());
                        outstandingPacketContainer.add(outstandingPacket);
                    }
                } else {
                    // Because batch msg is sent from Pulsar client, so only individual msg may have mqtt-5 properties.
                    MqttPublishMessage firstMessage = messages.get(0);
                    long expiryInterval = getMessageExpiryInterval(firstMessage);
                    boolean addToOutstandingPacketContainer = expiryInterval >= 0;
                    if (expiryInterval < 0) {
                        log.warn("mqtt msg has expired : {}", firstMessage);
                        messages.remove(0);
                        getSubscription().acknowledgeMessage(
                                Collections.singletonList(entry.getPosition()),
                                CommandAck.AckType.Individual, Collections.emptyMap());
                    }
                    if (addToOutstandingPacketContainer) {
                        OutstandingPacket outstandingPacket = new OutstandingPacket(this,
                                messages.get(0).variableHeader().packetId(), entry.getLedgerId(), entry.getEntryId());
                        outstandingPacketContainer.add(outstandingPacket);
                    }
                }
            }
            for (MqttPublishMessage msg : messages) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] [{}] [{}] Send MQTT message {} to subscriber", pulsarTopicName,
                            mqttTopicName, super.getSubscription().getName(), msg);
                }
                final int readableBytes = msg.payload().readableBytes();
                metricsCollector.addReceived(readableBytes);
                if (clientRestrictions.exceedMaximumPacketSize(readableBytes)) {
                    log.warn("discard msg {}, because it exceeds maximum packet size : {}, msg size {}", msg,
                            clientRestrictions.getMaximumPacketSize(), readableBytes);
                    getSubscription().acknowledgeMessage(Collections.singletonList(entry.getPosition()),
                            CommandAck.AckType.Individual, Collections.emptyMap());
                    continue;
                }
                cnx.ctx().channel().write(new MqttAdapterMessage(connection.getClientId(), msg,
                        connection.isFromProxy()));
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
        if (var > clientRestrictions.getReceiveMaximum() / 2) {
            MESSAGE_PERMITS_UPDATER.addAndGet(this, var);
            this.getSubscription().consumerFlow(this, availablePermits);
            ADD_PERMITS_UPDATER.set(this, 0);
        }
    }

    public void addAllPermits() {
        this.availablePermits = clientRestrictions.getReceiveMaximum();
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
