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

import static io.streamnative.pulsar.handlers.mqtt.Constants.AUTH_BASIC;
import static io.streamnative.pulsar.handlers.mqtt.Constants.AUTH_TOKEN;
import com.google.common.collect.ImmutableList;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationDataCommand;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.AuthenticationService;


@Slf4j
public class AuthUtils {
  public static final List<String> SUPPORTED_AUTH_METHODS = ImmutableList.of(AUTH_BASIC, AUTH_TOKEN);

  public static AuthenticationDataSource getAuthData(String authMethod, MqttConnectPayload payload) {
    switch (authMethod) {
      case AUTH_BASIC:
        return new AuthenticationDataCommand(payload.userName() + ":" + payload.password());
      case AUTH_TOKEN:
        return new AuthenticationDataCommand(payload.password());
      default:
        throw new IllegalStateException("Got invalid or unsupported authentication method!");
    }
  }

  public static Map<String, AuthenticationProvider> configureAuthProviders(
      AuthenticationService authService, List<String> authMethods) {
    final HashMap<String, AuthenticationProvider> authProviders = new HashMap<>();
    for (String authMethod : authMethods) {
      if (!SUPPORTED_AUTH_METHODS.contains(authMethod)) {
        log.error("MQTT authentication method {} is not supported and will be ignored!", authMethod);
        continue;
      }
      final AuthenticationProvider authProvider = authService.getAuthenticationProvider(authMethod);
      if (authProvider != null) {
        authProviders.put(authMethod, authProvider);
      } else {
        log.error("MQTT authentication method {} is not enabled in Pulsar configuration!", authMethod);
      }
    }
    if (authProviders.isEmpty()) {
      throw new IllegalStateException(
          "MQTT authentication is enabled but no providers were successfully configured");
    }
    return authProviders;
  }
}
