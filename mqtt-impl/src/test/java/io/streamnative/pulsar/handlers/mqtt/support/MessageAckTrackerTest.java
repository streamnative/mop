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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import io.streamnative.pulsar.handlers.mqtt.proxy.MessageAckTracker;
import org.testng.annotations.Test;

public class MessageAckTrackerTest {

    @Test
    public void testTracker() {
        MessageAckTracker messageAckTracker = new MessageAckTracker();
        final int messageId = 11111;
        messageAckTracker.increment(messageId);
        messageAckTracker.increment(messageId);
        assertFalse(messageAckTracker.decrementAndCheck(messageId));
        assertTrue(messageAckTracker.decrementAndCheck(messageId));
        final int unknownMessageId = 123;
        assertFalse(messageAckTracker.decrementAndCheck(unknownMessageId));
    }
}
