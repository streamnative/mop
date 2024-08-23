/**
 * Copyright (c) 2020 StreamNative, Inc.. All Rights Reserved.
 */
package io.streamnative.pulsar.handlers.mqtt.oidc;

import lombok.Getter;

public class PoolCompiler {

    private final Pool pool;

    @Getter
    private final ExpressionCompiler compiler;

    public PoolCompiler(Pool pool) {
        this.pool = pool;
        this.compiler = new ExpressionCompiler(pool.expression());
    }

    public String getExpression() {
        return pool.expression();
    }

    public String getName() {
        return pool.name();
    }

    public String getProviderName() {
        return pool.providerName();
    }

}
