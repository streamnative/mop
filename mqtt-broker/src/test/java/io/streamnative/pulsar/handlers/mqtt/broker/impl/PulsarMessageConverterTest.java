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
package io.streamnative.pulsar.handlers.mqtt.broker.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.ReferenceCountUtil;
import io.streamnative.pulsar.handlers.mqtt.common.mqtt5.PacketIdGenerator;
import java.util.List;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.Test;

public class PulsarMessageConverterTest {

    @Test
    public void testMqttPayloadReleaseDoesNotReleaseEntry() {
        ByteBuf payload = Unpooled.copiedBuffer("payload", UTF_8);
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(1)
                .setPublishTime(System.currentTimeMillis());
        ByteBuf metadataAndPayload = Commands.serializeMetadataAndPayload(Commands.ChecksumType.None,
                metadata, payload);
        EntryImpl entry = EntryImpl.create(PositionFactory.create(1, 1), metadataAndPayload);
        metadataAndPayload.release();
        payload.release();

        List<MqttPublishMessage> messages = PulsarMessageConverter.toMqttMessages("mqtt/topic", entry,
                PacketIdGenerator.newNonZeroGenerator(), MqttQoS.AT_MOST_ONCE);

        assertEquals(messages.size(), 1);
        MqttPublishMessage message = messages.get(0);
        assertEquals(message.payload().toString(UTF_8), "payload");

        ReferenceCountUtil.release(message);
        assertEquals(entry.refCnt(), 1);

        entry.release();
    }
}
