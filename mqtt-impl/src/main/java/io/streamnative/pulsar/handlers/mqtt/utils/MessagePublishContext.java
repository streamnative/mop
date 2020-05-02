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
package io.streamnative.pulsar.handlers.mqtt.utils;

import static io.streamnative.pulsar.handlers.mqtt.utils.PulsarMessageConverter.messageToByteBuf;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.pulsar.client.api.Message;

/**
 * Implementation for PublishContext.
 */
@Slf4j
public final class MessagePublishContext implements PublishContext {

    private Topic topic;
    private long startTimeNs;
    private CompletableFuture<PositionImpl> positionFuture;

    /**
     * Executed from managed ledger thread when the message is persisted.
     */
    @Override
    public void completed(Exception exception, long ledgerId, long entryId) {
        if (exception != null) {
            log.error("Failed write entry: ledgerId: {}, entryId: {}. triggered send callback.", ledgerId, entryId);
            positionFuture.completeExceptionally(exception);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Success write topic: {}, ledgerId: {}, entryId: {}. triggered send callback.",
                        topic.getName(), ledgerId, entryId);
            }
            topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.MICROSECONDS);
            positionFuture.complete(PositionImpl.get(ledgerId, entryId));
        }
        recycle();
    }

    // recycler
    public static MessagePublishContext get(CompletableFuture<PositionImpl> positionFuture, Topic topic,
                                                                                 long startTimeNs) {
        MessagePublishContext callback = RECYCLER.get();
        callback.positionFuture = positionFuture;
        callback.topic = topic;
        callback.startTimeNs = startTimeNs;
        return callback;
    }

    private final Handle<MessagePublishContext> recyclerHandle;

    private MessagePublishContext(Handle<MessagePublishContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<MessagePublishContext> RECYCLER = new Recycler<MessagePublishContext>() {
        protected MessagePublishContext newObject(Handle<MessagePublishContext> handle) {
            return new MessagePublishContext(handle);
        }
    };

    public void recycle() {
        topic = null;
        startTimeNs = -1;
        recyclerHandle.recycle(this);
    }

    /**
     * publish amqp message to pulsar topic, no batch.
     */
    public static CompletableFuture<PositionImpl> publishMessages(Message<byte[]> message, Topic topic) {
        CompletableFuture<PositionImpl> future = new CompletableFuture<>();

        ByteBuf headerAndPayload = messageToByteBuf(message);
        topic.publishMessage(headerAndPayload,
                MessagePublishContext.get(future, topic, System.nanoTime()));

        return future;
    }
}
