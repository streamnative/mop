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

/**
 * Packed ID generator.
 */
public interface PacketIdGenerator {

    int MAX_MESSAGE_IN_FLIGHT = 65535;

    static PacketIdGenerator newNonZeroGenerator() {
        return new NonZeroPackedIdGenerator();
    }

    /**
     * Get next packet ID.
     * @return packet ID.
     */
    int nextPacketId();

    /**
     * Non zero packet ID generator implementation.
     */
    class NonZeroPackedIdGenerator implements PacketIdGenerator {

        private int lastPacketId;

        public synchronized int nextPacketId() {
            final int nextPacketId = ++lastPacketId;
            if (nextPacketId >= MAX_MESSAGE_IN_FLIGHT) {
                lastPacketId = 0;
            }
            return nextPacketId;
        }
    }
}
