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
package io.streamnative.pulsar.handlers.mqtt.support.systemtopic;

import java.util.concurrent.CompletableFuture;

public interface SystemEventService {

    void start();

    void close();

    void addListener(EventListener listener);

    CompletableFuture<Void> sendConnectEvent(ConnectEvent event);

    CompletableFuture<Void> sendLWTEvent(LastWillMessageEvent event);

    CompletableFuture<Void> sendEvent(MqttEvent event);
}
