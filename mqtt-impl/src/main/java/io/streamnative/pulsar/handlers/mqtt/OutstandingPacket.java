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
import java.util.Objects;
import lombok.Getter;

/**
 * Outstanding packet that the broker sent to clients.
 */

@Getter
public class OutstandingPacket {

    private final MQTTConsumer consumer;
    private final int packetId;
    private final long ledgerId;
    private final long entryId;
    private final int batchIndex;

    private final int batchSize;

    public OutstandingPacket(MQTTConsumer consumer, int packetId, long ledgerId, long entryId) {
        this.consumer = consumer;
        this.packetId = packetId;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.batchIndex = -1;
        this.batchSize = -1;
    }

    public OutstandingPacket(MQTTConsumer consumer, int packetId, long ledgerId,
                             long entryId, int batchIndex, int batchSize) {
        this.consumer = consumer;
        this.packetId = packetId;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.batchIndex = batchIndex;
        this.batchSize = batchSize;
    }

    public boolean isBatch() {
        return batchIndex != -1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OutstandingPacket that = (OutstandingPacket) o;
        return packetId == that.packetId && ledgerId == that.ledgerId
                && entryId == that.entryId && batchIndex == that.batchIndex && batchSize == that.batchSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(packetId, ledgerId, entryId, batchIndex, batchSize);
    }
}
