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

import io.streamnative.pulsar.handlers.mqtt.support.MQTTConsumer;

/**
 * Outstanding packet that the broker sent to clients.
 */
public class OutstandingPacket {

    private final MQTTConsumer consumer;
    private final int packetId;
    private final long ledgerId;
    private final long entryId;


    public OutstandingPacket(MQTTConsumer consumer, int packetId, long ledgerId, long entryId) {
        this.consumer = consumer;
        this.packetId = packetId;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    public MQTTConsumer getConsumer() {
        return consumer;
    }

    public int getPacketId() {
        return packetId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }
}
