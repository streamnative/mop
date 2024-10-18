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
package io.streamnative.pulsar.handlers.mqtt.broker.support;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


public class TopicAliasManager {

    public static final int NO_ALIAS = -1;
    private final Map<Integer, String> clientSideAlias;
    private final Map<String, TopicAliasInfo> brokerSideAlias;

    private final PacketIdGenerator idGenerator;
    @Getter
    private final int topicMaximumAlias;

    public TopicAliasManager(int topicMaximumAlias) {
        this.topicMaximumAlias = topicMaximumAlias;
        this.idGenerator = PacketIdGenerator.newNonZeroGenerator();
        this.clientSideAlias = new ConcurrentHashMap<>(topicMaximumAlias);
        this.brokerSideAlias = new ConcurrentHashMap<>(topicMaximumAlias);
    }

    public Optional<String> getTopicByAlias(int alias) {
        return Optional.ofNullable(clientSideAlias.get(alias));
    }

    public boolean updateTopicAlias(String topic, int alias) {
        return clientSideAlias.compute(alias, (k, v) -> {
            if (v == null && clientSideAlias.size() >= topicMaximumAlias) {
                return null;
            }
            return topic;
        }) != null;
    }

    public Optional<Integer> getOrCreateAlias(String topic) {
        TopicAliasInfo topicAliasInfo = brokerSideAlias.computeIfAbsent(topic, (k) -> {
            if (brokerSideAlias.size() >= topicMaximumAlias) {
                return null;
            }
            return TopicAliasInfo.builder()
                    .alias(idGenerator.nextPacketId())
                    .syncToClient(false)
                    .build();
        });
        if (topicAliasInfo == null) {
            // Exceeds topic alias maximum
            return Optional.empty();
        }
        return Optional.of(topicAliasInfo.getAlias());
    }

    public boolean isSyncAliasToClient(String topicName) {
        TopicAliasInfo topicAliasInfo = brokerSideAlias.get(topicName);
        if (topicAliasInfo == null) {
            return false;
        }
        return topicAliasInfo.isSyncToClient();
    }

    public void syncAlias(String topic) {
        brokerSideAlias.computeIfPresent(topic, (key, info) -> {
            info.setSyncToClient(true);
            return info;
        });
    }

    @Getter
    @ToString
    @EqualsAndHashCode
    @Builder
    static class TopicAliasInfo {
        private int alias;
        @Setter
        private boolean syncToClient;
    }
}
