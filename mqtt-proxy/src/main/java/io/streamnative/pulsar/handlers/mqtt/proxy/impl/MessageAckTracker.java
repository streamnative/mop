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
package io.streamnative.pulsar.handlers.mqtt.proxy.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageAckTracker {
    private final Map<Integer, AtomicInteger> messageIdCounter;

    public MessageAckTracker() {
        this.messageIdCounter = new ConcurrentHashMap<>();
    }

    public void increment(int messageId) {
        increment(messageId, 1);
    }

    public void increment(int messageId, int count) {
        messageIdCounter.computeIfAbsent(messageId,
                (k) -> new AtomicInteger(0)).addAndGet(count);
    }

    public boolean checkIfSendAck(int messageId) {
        AtomicInteger counter = messageIdCounter.get(messageId);
        if (counter == null || counter.decrementAndGet() > 0) {
            return false;
        }
        return messageIdCounter.remove(messageId) != null;
    }

    public void remove(int messageId) {
        messageIdCounter.remove(messageId);
    }
}
