/**
 * Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.utils;

import lombok.experimental.UtilityClass;

import javax.validation.constraints.NotNull;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@UtilityClass
public final class Paths {

    public String getUrlEncodedPath(@NotNull String name) {
        return URLEncoder.encode(name, StandardCharsets.UTF_8);
    }

    public String getUrlDecodedPath(@NotNull String name) {
        return URLDecoder.decode(name, StandardCharsets.UTF_8);
    }
}
