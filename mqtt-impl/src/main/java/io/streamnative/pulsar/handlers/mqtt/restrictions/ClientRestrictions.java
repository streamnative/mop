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
package io.streamnative.pulsar.handlers.mqtt.restrictions;

import io.streamnative.pulsar.handlers.mqtt.exception.restrictions.InvalidSessionExpireIntervalException;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Builder
@ToString
@EqualsAndHashCode
public class ClientRestrictions {

    private static final int DEFAULT_RECEIVE_MAXIMUM = 1000;

    private Integer sessionExpireInterval;
    private Integer receiveMaximum;
    private Integer keepAliveTime;
    @Getter
    private boolean cleanSession;
    private Integer maximumPacketSize;

    private Integer topicAliasMaximum;
    @Getter
    private boolean allowReasonStrOrUserProperty;

    public int getSessionExpireInterval() {
        return Optional.ofNullable(sessionExpireInterval)
                .orElse(SessionExpireInterval.EXPIRE_IMMEDIATELY.getSecondTime());
    }

    public int getReceiveMaximum() {
        return Optional.ofNullable(receiveMaximum).orElse(DEFAULT_RECEIVE_MAXIMUM);
    }

    public int getKeepAliveTime() {
        return Optional.ofNullable(keepAliveTime).orElse(0);
    }

    public int getMaximumPacketSize() {
        return Optional.ofNullable(maximumPacketSize).orElse(0);
    }

    public boolean exceedMaximumPacketSize(int readableBytes) {
        return getMaximumPacketSize() != 0 ? readableBytes > maximumPacketSize : false;
    }

    public int getTopicAliasMaximum() {
        return Optional.ofNullable(topicAliasMaximum).orElse(0);
    }

    public void updateExpireInterval(int newExpireInterval) throws InvalidSessionExpireIntervalException {
        if (sessionExpireInterval <= SessionExpireInterval.EXPIRE_IMMEDIATELY.getSecondTime()
                || newExpireInterval < SessionExpireInterval.EXPIRE_IMMEDIATELY.getSecondTime()) {
            throw new InvalidSessionExpireIntervalException(
                    String.format("Illegal session expire interval originValue=%s newValue=%s",
                            sessionExpireInterval, newExpireInterval));
        } else {
            sessionExpireInterval = newExpireInterval;
        }
    }

    public boolean isSessionExpireImmediately() {
        return sessionExpireInterval == null
                || sessionExpireInterval == SessionExpireInterval.EXPIRE_IMMEDIATELY.getSecondTime();
    }

    public boolean isSessionNeverExpire() {
        return sessionExpireInterval == null
                || sessionExpireInterval == SessionExpireInterval.NEVER_EXPIRE.getSecondTime();
    }

    @Getter
    @AllArgsConstructor
    enum SessionExpireInterval {
        NEVER_EXPIRE(0xFFFFFFFF),
        EXPIRE_IMMEDIATELY(0);

        private final int secondTime;
    }
}
