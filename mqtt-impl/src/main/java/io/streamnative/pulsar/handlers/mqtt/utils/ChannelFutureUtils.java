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
package io.streamnative.pulsar.handlers.mqtt.utils;

import io.netty.channel.ChannelFuture;
import java.util.concurrent.CompletableFuture;

public class ChannelFutureUtils {

    /**
     * Wrapper Netty ChannelFuture to CompletableFuture.
     * @param channelFuture Netty ChannelFuture
     * @return CompletableFuture
     */
    public static CompletableFuture<Void> convertToCompletableFuture(ChannelFuture channelFuture) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        channelFuture.addListener(result -> {
            if (result.isSuccess()) {
                future.complete(null);
            } else {
                future.completeExceptionally(result.cause());
            }
        });
        return future;
    }
}
