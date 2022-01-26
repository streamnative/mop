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
    // describe by mqtt 5.0 version
    private static final int MQTT5_DEFAULT_RECEIVE_MAXIMUM = 65535;
    public static final int BEFORE_DEFAULT_RECEIVE_MAXIMUM = 1000;

    private Integer sessionExpireInterval;
    private Integer clientReceiveMaximum;
    private Integer keepAliveTime;
    @Getter
    private boolean cleanSession;

    public int getSessionExpireInterval() {
        return Optional.ofNullable(sessionExpireInterval)
                .orElse(SessionExpireInterval.EXPIRE_IMMEDIATELY.getSecondTime());
    }

    public int getClientReceiveMaximum() {
        return Optional.ofNullable(clientReceiveMaximum).orElse(MQTT5_DEFAULT_RECEIVE_MAXIMUM);
    }

    public int getKeepAliveTime() {
        return Optional.ofNullable(keepAliveTime).orElse(0);
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
        return sessionExpireInterval == SessionExpireInterval.EXPIRE_IMMEDIATELY.getSecondTime();
    }

    public boolean isSessionNeverExpire() {
        return sessionExpireInterval == SessionExpireInterval.NEVER_EXPIRE.getSecondTime();
    }

    @Getter
    @AllArgsConstructor
    enum SessionExpireInterval {
        NEVER_EXPIRE(0xFFFFFFFF),
        EXPIRE_IMMEDIATELY(0);

        private final int secondTime;
    }
}
