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
package io.streamnative.pulsar.handlers.mqtt.common.exception;

import lombok.Getter;

public class MQTTTopicAliasExceedsLimitException extends RuntimeException {
    @Getter
    private final int alias;
    @Getter
    private final int topicAliasMaximum;

    public MQTTTopicAliasExceedsLimitException(int alias, int topicAliasMaximum) {
        super(String.format("The topic alias %s is exceed topic alias maximum %s.", alias, topicAliasMaximum));
        this.alias = alias;
        this.topicAliasMaximum = topicAliasMaximum;
    }
}
