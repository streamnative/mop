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
import static io.streamnative.pulsar.handlers.mqtt.Constants.MTLS;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.ssl.SslHandler;
import io.streamnative.pulsar.handlers.mqtt.exception.MQTTAuthException;
import io.streamnative.pulsar.handlers.mqtt.oidc.OIDCService;
import io.streamnative.pulsar.handlers.mqtt.utils.MqttMessageUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.AuthenticationException;
import javax.net.ssl.SSLPeerUnverifiedException;
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

    public AuthenticationResult mTlsAuthenticate(SslHandler sslHandler, OIDCService oidcService)
            throws MQTTAuthException{
        try {
            if (sslHandler != null) {
                SSLSession session = sslHandler.engine().getSession();
                java.security.cert.Certificate[] clientCerts = session.getPeerCertificates();
                if (clientCerts.length > 0) {
                    if (clientCerts[0] instanceof java.security.cert.X509Certificate) {
                        java.security.cert.X509Certificate clientCert =
                                (java.security.cert.X509Certificate) clientCerts[0];
                        log.info("[proxy]-Client cert info: " + clientCert.getSubjectDN());
                        String[] parts = clientCert.getSubjectDN().toString().split("=");
                        Map<String, Object> datas = new HashMap<>();
                        datas.put(parts[0], parts[1]);
                        final String principal = oidcService.getPrincipal(datas);
                        log.info("[proxy]-mTls principal : {}", principal);
                        return new AuthenticationResult(true, principal,
                                new AuthenticationDataCommand(clientCert.getSubjectDN().toString()));
                    }
                }
            } else {
                String msg = "mTlsEnabled, but not find SslHandler, disconnect the connection";
                log.error("mTlsEnabled, but not find SslHandler, disconnect the connection");
                throw new MQTTAuthException(msg);
            }
        } catch (SSLPeerUnverifiedException ex) {
            log.warn("[proxy]- get client clientCerts error", ex);
            throw new MQTTAuthException(ex);
        }
        String msg = "mTlsEnabled, but not find matched principal";
        throw new MQTTAuthException(msg);
    }

    public AuthenticationResult authenticate(MqttConnectMessage connectMessage) {
        String authMethod = MqttMessageUtils.getAuthMethod(connectMessage);
        if (authMethod != null) {
            byte[] authData = MqttMessageUtils.getAuthData(connectMessage);
            if (authData == null) {
                return AuthenticationResult.FAILED;
            }
            return authenticate(connectMessage.payload().clientIdentifier(), authMethod,
                    new AuthenticationDataCommand(new String(authData)));
        }
        return authenticate(connectMessage.payload());
    }

    public AuthenticationResult authenticate(MqttConnectPayload payload) {
        String userRole = null;
        boolean authenticated = false;
        AuthenticationDataSource authenticationDataSource = null;
        for (Map.Entry<String, AuthenticationProvider> entry : authenticationProviders.entrySet()) {
            String authMethod = entry.getKey();
            try {
                AuthenticationDataSource authData = getAuthData(authMethod, payload);
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
        if (MTLS.equalsIgnoreCase(authMethod)) {
            return new AuthenticationResult(true, command.getCommandData(), command);
        }
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

        public static final AuthenticationResult FAILED = new AuthenticationResult(false, null, null);
        private final boolean authenticated;
        private final String userRole;
        private final AuthenticationDataSource authData;

        public boolean isFailed() {
            return !authenticated;
        }
    }
}
