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
package io.streamnative.pulsar.handlers.mqtt.common.utils;

import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.Codec;

public class EventParserUtils {
    public static TopicName parseTopicNameFromManagedLedgerEvent(String path) {
        // /managed-ledgers/public/default/persistent/topicName
        String[] pathArr = path.split("/");
        if (pathArr.length != 6) {
            // Avoid the existence of child nodes after the path to cause an exception
            // e.g: cursor path: /managed-ledgers/public/default/persistent/topicName/cusorname
            throw new IllegalArgumentException("Illegal argument path " + path);
        }
        String tenant = pathArr[2];
        String namespace = pathArr[3];
        String topicDomain = pathArr[4];
        String topicName = pathArr[5];
        String decodedTopicName = Codec.decode(topicName);
        return TopicName.get(topicDomain, tenant, namespace, decodedTopicName);
    }
}
