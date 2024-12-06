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
package io.streamnative.oidc.broker.common.pojo;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import javax.validation.constraints.NotNull;

public record Pool(@JsonProperty(value = "name", required = true) @NotNull String name,
                   @JsonProperty(value = "auth_type", defaultValue = AUTH_TYPE_TOKEN) @NotNull String authType,
                   @JsonProperty(value = "description", required = true) @NotNull String description,
                   @JsonProperty(value = "provider_name") @NotNull String providerName,
                   @JsonProperty(value = "expression", required = true) @NotNull String expression) {

    public static final String AUTH_TYPE_TOKEN = "token";
    public static final String AUTH_TYPE_MTLS = "mtls";

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pool pool = (Pool) o;
        return Objects.equals(name, pool.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    public String authType() {
        return (authType == null || authType.isEmpty()) ? AUTH_TYPE_TOKEN : authType;
    }
}
