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
package io.streamnative.pulsar.handlers.mqtt.common.authentication;

import static io.streamnative.pulsar.handlers.mqtt.common.Constants.AUTH_BASIC;
import static io.streamnative.pulsar.handlers.mqtt.common.Constants.AUTH_MTLS;
import static io.streamnative.pulsar.handlers.mqtt.common.Constants.AUTH_TOKEN;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.streamnative.pulsar.handlers.mqtt.common.utils.MqttMessageUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLSession;
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

    public AuthenticationResult authenticate(boolean fromProxy,
                                             SSLSession session, MqttConnectMessage connectMessage) {
        if (fromProxy) {
            return AuthenticationResult.PASSED;
        }
        String authMethod = MqttMessageUtils.getAuthMethod(connectMessage);
        if (authMethod != null) {
            byte[] authData = MqttMessageUtils.getAuthData(connectMessage);
            if (authData == null) {
                return AuthenticationResult.FAILED;
            }

            return authenticate(connectMessage.payload().clientIdentifier(), authMethod,
                    new AuthenticationDataCommand(new String(authData), null, session));
        }
        return authenticate(connectMessage.payload(), session);
    }

    public AuthenticationResult authenticate(MqttConnectPayload payload, SSLSession session) {
        String userRole = null;
        boolean authenticated = false;
        AuthenticationDataSource authenticationDataSource = null;
        for (Map.Entry<String, AuthenticationProvider> entry : authenticationProviders.entrySet()) {
            String authMethod = entry.getKey();
            try {
                AuthenticationDataSource authData = getAuthData(authMethod, payload, session);
                userRole = entry.getValue().authenticate(authData);
                authenticated = true;
                authenticationDataSource = authData;
                break;
            } catch (AuthenticationException e) {
                log.warn("Authentication failed with method: {}. CId={}, username={}",
                        authMethod, payload.clientIdentifier(), payload.userName());
            }
        }
        return new AuthenticationResult(authenticated, userRole, authenticationDataSource);
    }

    public AuthenticationResult authenticate(String clientIdentifier,
                                              String authMethod,
                                              AuthenticationDataCommand command) {
        AuthenticationProvider authenticationProvider = authenticationProviders.get(authMethod);
        if (authenticationProvider == null) {
            log.warn("Authentication failed, no authMethod : {} for CId={}", clientIdentifier, authMethod);
            return AuthenticationResult.FAILED;
        }
        String userRole = null;
        boolean authenticated = false;
        try {
            userRole = authenticationProvider.authenticate(command);
            authenticated = true;
        } catch (AuthenticationException e) {
            log.warn("Authentication failed for CId={}", clientIdentifier);
        }
        return new AuthenticationResult(authenticated, userRole, command);
    }

    public AuthenticationDataSource getAuthData(String authMethod, MqttConnectPayload payload, SSLSession session) {
        switch (authMethod) {
            case AUTH_BASIC:
                return new AuthenticationDataCommand(payload.userName() + ":" + payload.password());
            case AUTH_TOKEN:
                return new AuthenticationDataCommand(payload.password());
            case AUTH_MTLS:
                return new AuthenticationDataCommand(null, null, session);
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported authentication method : %s!", authMethod));
        }
    }

    @Getter
    @RequiredArgsConstructor
    public static class AuthenticationResult {

        public static final AuthenticationResult FAILED = new AuthenticationResult(false, null, null);
        public static final AuthenticationResult PASSED = new AuthenticationResult(true, null, null);
        private final boolean authenticated;
        private final String userRole;
        private final AuthenticationDataSource authData;

        public boolean isFailed() {
            return !authenticated;
        }
    }
}
