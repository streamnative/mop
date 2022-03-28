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

import org.apache.pulsar.common.naming.TopicName;

public class EventParserUtils {
    public static TopicName parseFromManagedLedgerEvent(String path) {
        // /managed-ledgers/public/default/persistent/topicName
        String[] pathArr = path.split("/");
        if (pathArr.length < 5) {
            throw new IllegalArgumentException("Illegal argument path " + path);
        }
        String tenant = pathArr[2];
        String namespace = pathArr[3];
        String topicDomain = pathArr[4];
        String topicName = pathArr[5];
        return TopicName.get(topicDomain, tenant, namespace, topicName);
    }
}
