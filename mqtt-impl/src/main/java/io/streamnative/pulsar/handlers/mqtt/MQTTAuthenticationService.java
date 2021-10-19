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

import static io.streamnative.pulsar.handlers.mqtt.Constants.AUTH_BASIC;
import static io.streamnative.pulsar.handlers.mqtt.Constants.AUTH_TOKEN;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.AuthenticationException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;

/**
 * MQTT authentication service.
 */
@Slf4j
public class MQTTAuthenticationService {

    @Getter
    private final AuthenticationService authenticationService;

    @Getter
    private final Map<String, AuthenticationProvider> authenticationProviders;

    public MQTTAuthenticationService(AuthenticationService authenticationService, List<String> authenticationMethods) {
        this.authenticationService = authenticationService;
        this.authenticationProviders = getAuthenticationProviders(authenticationMethods);
    }

    private Map<String, AuthenticationProvider> getAuthenticationProviders(List<String> authenticationMethods) {
        Map<String, AuthenticationProvider> providers = new HashMap<>();
        for (String method : authenticationMethods) {
            final AuthenticationProvider authProvider = authenticationService.getAuthenticationProvider(method);
            if (authProvider != null) {
                providers.put(method, authProvider);
            } else {
                log.error("MQTT authentication method {} is not enabled in Pulsar configuration!", method);
            }
        }
        if (providers.isEmpty()) {
            throw new IllegalArgumentException(
                    "MQTT authentication is enabled but no providers were successfully configured");
        }
        return providers;
    }

    public AuthenticationResult authenticate(MqttConnectPayload payload) {
        String userRole = null;
        boolean authenticated = false;
        for (Map.Entry<String, AuthenticationProvider> entry : authenticationProviders.entrySet()) {
            String authMethod = entry.getKey();
            try {
                userRole = entry.getValue().authenticate(getAuthData(authMethod, payload));
                authenticated = true;
                break;
            } catch (AuthenticationException e) {
                log.warn("Authentication failed with method: {}. CId={}, username={}",
                        authMethod, payload.clientIdentifier(), payload.userName());
            }
        }
        return new AuthenticationResult(authenticated, userRole);
    }

    public AuthenticationDataSource getAuthData(String authMethod, MqttConnectPayload payload) {
        switch (authMethod) {
            case AUTH_BASIC:
                return new AuthenticationDataCommand(payload.userName() + ":" + payload.password());
            case AUTH_TOKEN:
                return new AuthenticationDataCommand(payload.password());
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported authentication method : %s!", authMethod));
        }
    }

    @Getter
    @RequiredArgsConstructor
    public static class AuthenticationResult {
        private final boolean authenticated;
        private final String userRole;

        public boolean isFailed() {
            return !authenticated;
        }
    }
}
