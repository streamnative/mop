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

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.FastThreadLocal;
import io.streamnative.pulsar.handlers.mqtt.support.MessageBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Tools for converting MQTT message to Pulsar message and Pulsar message to MQTT message.
 */
@Slf4j
public class PulsarMessageConverter {

    private static final Schema<byte[]> SCHEMA = Schema.BYTES;
    private static final String FAKE_MQTT_PRODUCER_NAME = "fake_mqtt_producer_name";

    private static final FastThreadLocal<SingleMessageMetadata> LOCAL_SINGLE_MESSAGE_METADATA = //
            new FastThreadLocal<SingleMessageMetadata>() {
                @Override
                protected SingleMessageMetadata initialValue() throws Exception {
                    return new SingleMessageMetadata();
                }
            };

    private static final FastThreadLocal<MessageMetadata> LOCAL_MESSAGE_METADATA = //
            new FastThreadLocal<MessageMetadata>() {
                @Override
                protected MessageMetadata initialValue() throws Exception {
                    return new MessageMetadata();
                }
            };

    // Convert MQTT message to Pulsar message.
    public static MessageImpl<byte[]> toPulsarMsg(Topic topic, MqttPublishMessage mqttMsg) {
        MessageMetadata metadata = LOCAL_MESSAGE_METADATA.get();
        metadata.clear();
        return MessageImpl.create(metadata, mqttMsg.payload().nioBuffer(), SCHEMA, topic.getName());
    }

    public static List<MqttPublishMessage> toMqttMessages(String topicName, Entry entry, int messageId, MqttQoS qos) {
        ByteBuf metadataAndPayload = entry.getDataBuffer();
        MessageMetadata metadata = Commands.parseMessageMetadata(metadataAndPayload);
        if (metadata.hasNumMessagesInBatch()) {
            int batchSize = metadata.getNumMessagesInBatch();
            List<MqttPublishMessage> response = new ArrayList<>(batchSize);
            try {
                for (int i = 0; i < batchSize; i++) {
                    SingleMessageMetadata single = LOCAL_SINGLE_MESSAGE_METADATA.get();
                    single.clear();
                    ByteBuf singleMessagePayload = Commands.deSerializeSingleMessageInBatch(metadataAndPayload,
                            single,
                            i, batchSize);
                    response.add(MessageBuilder.publish()
                            .messageId(messageId)
                            .payload(singleMessagePayload)
                            .topicName(topicName)
                            .qos(qos)
                            .retained(false)
                            .build());
                }
                return response;
            } catch (IOException e) {
                log.error("Error decoding batch for message {}. Whole batch will be included in output",
                        entry.getPosition(), e);
                return Collections.emptyList();
            }
        } else {
            return Collections.singletonList(MessageBuilder.publish()
                    .messageId(messageId)
                    .payload(metadataAndPayload)
                    .topicName(topicName)
                    .qos(qos)
                    .retained(false)
                    .build());
        }
    }

    // convert message to ByteBuf payload for ledger.addEntry.
    // parameter message is converted from passed in MQTT message.
    // called when publish receives MQTT message to write into Pulsar.
    public static ByteBuf messageToByteBuf(Message<byte[]> message) {
        checkArgument(message instanceof MessageImpl);

        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) message;
        MessageMetadata metadata = LOCAL_MESSAGE_METADATA.get();
        metadata.clear();
        ByteBuf payload = msg.getDataBuffer();

        // filled in required fields
        if (!metadata.hasSequenceId()) {
            metadata.setSequenceId(-1);
        }
        if (!metadata.hasPublishTime()) {
            metadata.setPublishTime(System.currentTimeMillis());
        }
        if (!metadata.hasProducerName()) {
            metadata.setProducerName(FAKE_MQTT_PRODUCER_NAME);
        }

        metadata.setCompression(
                CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        metadata.setUncompressedSize(payload.readableBytes());

        ByteBuf buf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);

        return buf;
    }

}
