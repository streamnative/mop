package io.github.nmop.configuration;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@EqualsAndHashCode
@ToString
public final class Configuration {
    private String listeners;
    private String defaultNamespace;
    private boolean autoClientIdentifier;
}
