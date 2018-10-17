/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.r2dbc.adba;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import reactor.core.publisher.Mono;

/**
 * R2DBC wrapper for a {@link jdk.incubator.sql2.Connection ADBA Connection}.
 *
 * @author Mark Paluch
 * @see jdk.incubator.sql2.Connection
 */
class AdbaConnection implements Connection {

    private final jdk.incubator.sql2.Connection delegate;

    /**
     * Create a new {@link AdbaConnection} for an {@link jdk.incubator.sql2.Connection ADBA Connection}.
     *
     * @param delegate must not be {@literal null}.
     */
    private AdbaConnection(jdk.incubator.sql2.Connection delegate) {
        this.delegate = delegate;
    }

    /**
     * Create a new {@link AdbaConnection} for an {@link jdk.incubator.sql2.Connection ADBA Connection}.
     *
     * @param delegate must not be {@literal null}.
     * @return {@link AdbaConnection} for the {@link jdk.incubator.sql2.Connection ADBA Connection}.
     */
    public static AdbaConnection create(jdk.incubator.sql2.Connection delegate) {

        Assert.notNull(delegate, "Connection must not be null!");

        return new AdbaConnection(delegate);
    }

    @Override
    public Mono<Void> beginTransaction() {
        return Mono.empty();
    }

    @Override
    public Mono<Void> close() {
        return AdbaUtils.submitLater(delegate::closeOperation);
    }

    @Override
    public Mono<Void> commitTransaction() {
        return AdbaUtils.executeLater(delegate::commit).then();
    }

    @Override
    public Batch createBatch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Mono<Void> createSavepoint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AdbaStatement createStatement(String sql) {
        return AdbaStatement.create(delegate, sql);
    }

    @Override
    public Mono<Void> releaseSavepoint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Mono<Void> rollbackTransaction() {
        return AdbaUtils.executeLater(delegate::rollback).then();
    }

    @Override
    public Mono<Void> rollbackTransactionToSavepoint(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Mono<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
        throw new UnsupportedOperationException();
    }
}
