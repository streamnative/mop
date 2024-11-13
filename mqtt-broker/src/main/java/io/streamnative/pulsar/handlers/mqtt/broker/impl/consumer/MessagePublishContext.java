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
package io.streamnative.pulsar.handlers.mqtt.broker.impl.consumer;

import static io.streamnative.pulsar.handlers.mqtt.broker.impl.PulsarMessageConverter.messageToByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.client.api.Message;

/**
 * Implementation for PublishContext.
 */
@Slf4j
public final class MessagePublishContext implements PublishContext {

    private String producerName;
    private Topic topic;
    private long startTimeNs;
    private CompletableFuture<Position> positionFuture;
    private long sequenceId;

    /**
     * Executed from managed ledger thread when the message is persisted.
     */
    @Override
    public void completed(Exception exception, long ledgerId, long entryId) {
        if (exception != null) {
            log.error("Failed write entry: ledgerId: {}, entryId: {}.", ledgerId, entryId, exception);
            positionFuture.completeExceptionally(exception);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Success write topic: {}, ledgerId: {}, entryId: {}. triggered send callback.",
                        topic.getName(), ledgerId, entryId);
            }
            topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.NANOSECONDS);
            positionFuture.complete(PositionFactory.create(ledgerId, entryId));
        }
        recycle();
    }

    // recycler
    public static MessagePublishContext get(CompletableFuture<Position> positionFuture, String producerName,
                                            Topic topic, long sequenceId, long startTimeNs) {
        MessagePublishContext callback = RECYCLER.get();
        callback.positionFuture = positionFuture;
        callback.producerName = producerName;
        callback.topic = topic;
        callback.sequenceId = sequenceId;
        callback.startTimeNs = startTimeNs;
        return callback;
    }

    private final Handle<MessagePublishContext> recyclerHandle;

    private MessagePublishContext(Handle<MessagePublishContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public String getProducerName() {
        return producerName;
    }

    @Override
    public long getSequenceId() {
        return this.sequenceId;
    }


    private static final Recycler<MessagePublishContext> RECYCLER = new Recycler<MessagePublishContext>() {
        protected MessagePublishContext newObject(Handle<MessagePublishContext> handle) {
            return new MessagePublishContext(handle);
        }
    };

    public void recycle() {
        positionFuture = null;
        topic = null;
        startTimeNs = -1;
        sequenceId = -1;
        recyclerHandle.recycle(this);
    }

    /**
     * publish mqtt message to pulsar topic, no batch.
     */
    public static CompletableFuture<Position> publishMessages(String producerName, Message<byte[]> message,
                                                              long sequenceId, Topic topic) {
        CompletableFuture<Position> future = new CompletableFuture<>();

        ByteBuf headerAndPayload = messageToByteBuf(message);
        topic.publishMessage(headerAndPayload,
                MessagePublishContext.get(future, producerName, topic, sequenceId, System.nanoTime()));
        headerAndPayload.release();
        return future;
    }
}
