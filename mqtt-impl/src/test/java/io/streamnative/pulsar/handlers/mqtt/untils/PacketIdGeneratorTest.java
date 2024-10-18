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

package io.streamnative.pulsar.handlers.mqtt.untils;

import io.streamnative.pulsar.handlers.mqtt.broker.support.PacketIdGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PacketIdGeneratorTest {

    @Test
    public void testNextPacketId() {
        PacketIdGenerator packetIdGenerator = PacketIdGenerator.newNonZeroGenerator();
        long packetId = packetIdGenerator.nextPacketId();
        Assert.assertEquals(packetId, 1);
        for (int i = 0; i < PacketIdGenerator.MAX_MESSAGE_IN_FLIGHT - 1; i++) {
            packetId = packetIdGenerator.nextPacketId();
        }
        Assert.assertEquals(packetId, PacketIdGenerator.MAX_MESSAGE_IN_FLIGHT);
        packetId = packetIdGenerator.nextPacketId();
        Assert.assertEquals(packetId, 1);
    }
}
