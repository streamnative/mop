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
package io.streamnative.pulsar.handlers.mqtt.exception.handler;

import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTServerException;
import lombok.extern.slf4j.Slf4j;

/**
 * GlobalException to encapsulated exception handler.
 * We can use this method to achieve chained calls.
 * @see MqttExceptionHelper
 */
@Slf4j
public class GlobalExceptionHandler {
    private final boolean handled;
    private final Channel channel;
    private final Throwable ex;


    private GlobalExceptionHandler(boolean handled, Channel channel, Throwable ex) {
        this.handled = handled;
        this.channel = channel;
        this.ex = ex;
    }

    public static GlobalExceptionHandler handleServerException(Channel channel, Throwable ex) {
        boolean isHandled = false;
        if (ex instanceof MQTTServerException) {
            isHandled = MqttExceptionHelper.handleMqttServerException(channel, (MQTTServerException) ex);
        }
        return new GlobalExceptionHandler(isHandled, channel, ex);
    }

    public void orElseHandleCommon(MqttMessageType condition, int packetId) {
        if (handled) {
            return;
        }
        MqttExceptionHelper.handleCommonException(condition, channel, packetId, ex);
    }

}
