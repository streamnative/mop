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

package io.streamnative.pulsar.handlers.mqtt.support.event;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.SystemTopicClientBase;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class MQTTEventSystemTopicClient extends SystemTopicClientBase<MqttEvent> {

    public MQTTEventSystemTopicClient(PulsarClient client, TopicName topicName) {
        super(client, topicName);
    }

    @Override
    protected CompletableFuture<Writer<MqttEvent>> newWriterAsyncInternal() {
        return client.newProducer(Schema.JSON(MqttEvent.class))
                .topic(topicName.toString())
                .enableBatching(false)
                .createAsync().thenCompose(producer -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new mqtt-event-writer is created", topicName);
                    }
                    return CompletableFuture.completedFuture(new MQTTEventSystemTopicClient.EventWriter(producer,
                            MQTTEventSystemTopicClient.this));
                });
    }

    @Override
    protected CompletableFuture<Reader<MqttEvent>> newReaderAsyncInternal() {
        return client.newReader(Schema.JSON(MqttEvent.class))
                .topic(topicName.toString())
                .startMessageId(MessageId.earliest)
                .readCompacted(true).createAsync()
                .thenCompose(reader -> {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] A new mqtt-event-reader is created", topicName);
                    }
                    return CompletableFuture.completedFuture(new MQTTEventSystemTopicClient.EventReader(reader,
                            MQTTEventSystemTopicClient.this));
                });
    }

    private static class EventWriter implements Writer<MqttEvent> {

        private final Producer<MqttEvent> producer;
        private final SystemTopicClient<MqttEvent> systemTopicClient;

        private EventWriter(Producer<MqttEvent> producer, SystemTopicClient<MqttEvent> systemTopicClient) {
            this.producer = producer;
            this.systemTopicClient = systemTopicClient;
        }

        @Override
        public MessageId write(MqttEvent event) throws PulsarClientException {
            TypedMessageBuilder<MqttEvent> builder = producer.newMessage().key(event.getKey()).value(event);
            return builder.send();
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(MqttEvent event) {
            TypedMessageBuilder<MqttEvent> builder = producer.newMessage().key(event.getKey()).value(event);
            return builder.sendAsync();
        }

        @Override
        public MessageId delete(MqttEvent event) throws PulsarClientException {
            TypedMessageBuilder<MqttEvent> builder = producer.newMessage().key(event.getKey()).value(event);
            return builder.send();
        }

        @Override
        public CompletableFuture<MessageId> deleteAsync(MqttEvent event) {
            TypedMessageBuilder<MqttEvent> builder = producer.newMessage().key(event.getKey()).value(event);
            return builder.sendAsync();
        }

        @Override
        public void close() throws IOException {
            this.producer.close();
            systemTopicClient.getWriters().remove(MQTTEventSystemTopicClient.EventWriter.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return producer.closeAsync().thenCompose(v -> {
                systemTopicClient.getWriters().remove(MQTTEventSystemTopicClient.EventWriter.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<MqttEvent> getSystemTopicClient() {
            return systemTopicClient;
        }
    }

    private static class EventReader implements Reader<MqttEvent> {

        private final org.apache.pulsar.client.api.Reader<MqttEvent> reader;
        private final SystemTopicClient systemTopic;

        private EventReader(org.apache.pulsar.client.api.Reader<MqttEvent> reader, SystemTopicClient systemTopic) {
            this.reader = reader;
            this.systemTopic = systemTopic;
        }

        @Override
        public Message<MqttEvent> readNext() throws PulsarClientException {
            return reader.readNext();
        }

        @Override
        public CompletableFuture<Message<MqttEvent>> readNextAsync() {
            return reader.readNextAsync();
        }

        @Override
        public boolean hasMoreEvents() throws PulsarClientException {
            return reader.hasMessageAvailable();
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            return reader.hasMessageAvailableAsync();
        }

        @Override
        public void close() throws IOException {
            this.reader.close();
            systemTopic.getReaders().remove(MQTTEventSystemTopicClient.EventReader.this);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return reader.closeAsync().thenCompose(v -> {
                systemTopic.getReaders().remove(MQTTEventSystemTopicClient.EventReader.this);
                return CompletableFuture.completedFuture(null);
            });
        }

        @Override
        public SystemTopicClient<MqttEvent> getSystemTopic() {
            return systemTopic;
        }
    }
}
