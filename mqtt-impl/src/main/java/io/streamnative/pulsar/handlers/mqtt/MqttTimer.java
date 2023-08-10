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

import io.netty.util.HashedWheelTimer;
import net.jcip.annotations.ThreadSafe;

/**
 * The MQTT protocol timer container contains all of the
 * timers to help manage and observe them.
 */
@ThreadSafe
public class MqttTimer {
    private final static HashedWheelTimer COMMON_TIMER = new HashedWheelTimer();

    public static HashedWheelTimer getCommonTimer() {
        return COMMON_TIMER;
    }
}
