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
package io.streamnative.pulsar.handlers.mqtt.common;

import java.util.Arrays;

public class TopicFilterImpl implements TopicFilter {

    private final String[] filter;

    public TopicFilterImpl(String filterString) {
        String fs = filterString.endsWith(SPLITTER) ? filterString + SINGLE_LEVEL : filterString;
        this.filter = fs.split(SPLITTER);
    }

    @Override
    public boolean test(String localTopicName) {
        String topic = localTopicName.endsWith(SPLITTER) ? localTopicName + SINGLE_LEVEL : localTopicName;
        String[] parts = topic.split(SPLITTER);
        int boundary = Math.min(parts.length, filter.length);

        if (MULTI_LEVEL.equals(filter[filter.length - 1])) {
            boundary--;
            if (filter.length > parts.length) {
                return false;
            }
        } else if (filter.length != parts.length) {
            return false;
        }

        for (int i = 0; i < boundary; i++) {
            String f = filter[i];
            String v = parts[i];
            if (!(SINGLE_LEVEL.equals(f) || f.equals(v))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicFilterImpl that = (TopicFilterImpl) o;
        return Arrays.equals(filter, that.filter);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(filter);
    }
}
