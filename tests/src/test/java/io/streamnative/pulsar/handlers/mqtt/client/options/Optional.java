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
package io.streamnative.pulsar.handlers.mqtt.client.options;


import lombok.Data;

@Data
public class Optional<T> {
    private final boolean supported;
    private final boolean success;
    private final T body;

    private Optional(boolean supported, boolean success, T body) {
        this.supported = supported;
        this.success = success;
        this.body = body;
    }

    public static <T> Optional<T> of(T body) {
        return new Optional<T>(true, true, body);
    }

    public static <T> Optional<T> fail() {
        return new Optional<T>(true, false, null);
    }

    public static <T> Optional<T> unsupported() {
        return new Optional<T>(false, false, null);
    }
}
