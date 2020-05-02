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
import io.netty.handler.codec.mqtt.MqttQoS;
import io.streamnative.pulsar.handlers.mqtt.utils.PulsarMessageConverter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi;

/**
 * MQTT consumer.
 */
public class MQTTConsumer extends Consumer {

    private AtomicInteger msgId = new AtomicInteger(1);
    private final String topicName;
    private final MQTTServerCnx cnx;

    public MQTTConsumer(Subscription subscription, String topicName, String consumerName, MQTTServerCnx cnx)
            throws BrokerServiceException {
        super(subscription, PulsarApi.CommandSubscribe.SubType.Exclusive, topicName, 0, 0, consumerName, 0, cnx,
                "", null, false, PulsarApi.CommandSubscribe.InitialPosition.Latest, null);
        this.topicName = topicName;
        this.cnx = cnx;
    }

    @Override
    public ChannelPromise sendMessages(List<Entry> entries, EntryBatchSizes batchSizes, int totalMessages,
                                       long totalBytes, RedeliveryTracker redeliveryTracker) {
        ChannelPromise promise = cnx.ctx().newPromise();
        for (Entry entry : entries) {
            cnx.ctx().channel().write(PulsarMessageConverter.toMQTTMsg(topicName, entry, getMessageId(),
                    MqttQoS.AT_MOST_ONCE));
        }
        cnx.ctx().channel().writeAndFlush(Unpooled.EMPTY_BUFFER, promise);
        return promise;
    }

    private int getMessageId() {
        if (msgId.get() < Integer.MAX_VALUE) {
            msgId.incrementAndGet();
        } else {
            msgId.set(1);
        }
        return msgId.get();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int getAvailablePermits() {
        return 1000;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), msgId, topicName, cnx);
    }
}
