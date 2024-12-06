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
package io.streamnative.pulsar.handlers.mqtt.proxy.handler;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Lookup handler.
 */
public interface LookupHandler {

    /**
     * Find broker for protocolHandler.
     *
     * @param topicName topic name
     * @return Pair consist of brokerHost and brokerPort
     */
    CompletableFuture<InetSocketAddress> findBroker(TopicName topicName);

    /**
     * Close the lookup handler to cleanup the resource.
     */
    void close();
}
