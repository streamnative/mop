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

import io.streamnative.pulsar.handlers.mqtt.OutstandingPacket;
import io.streamnative.pulsar.handlers.mqtt.OutstandingPacketContainer;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Outstanding packet container implementation.
 */
public class OutstandingPacketContainerImpl implements OutstandingPacketContainer {

    ConcurrentHashMap<Integer, OutstandingPacket> packets = new ConcurrentHashMap<>();

    @Override
    public void add(OutstandingPacket packet) {
        packets.put(packet.getPacketId(), packet);
    }

    @Override
    public OutstandingPacket remove(int packetId) {
        return packets.remove(packetId);
    }

    @Override
    public OutstandingPacket getPacket(int packetId) {
        return packets.get(packetId);
    }
}
