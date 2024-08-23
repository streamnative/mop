/**
 * Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.oidc;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.Objects;

public record Pool(@JsonProperty(value = "name", required = true) String name,
                   @JsonProperty(value = "description", required = true) @NotNull String description,
                   @JsonProperty(value = "provider_name", required = true) @NotNull String providerName,
                   @JsonProperty(value = "expression", required = true) @NotNull String expression) {

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
}
