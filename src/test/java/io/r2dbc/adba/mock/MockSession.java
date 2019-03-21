/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.r2dbc.adba.mock;

import jdk.incubator.sql2.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collector;

/**
 * Mock implementation of {@link Connection}.
 * Operations for connecting and closing are shared across the {@link Connection} instance and can be {@link Operation#submit() submitted} multiple times.
 * <p>
 * SQL-based {@link Operation operations} are recorded by this connection for later introspection. Code interested in operation creation notifications (e.g. to customize an operation) can {@link #registerOnCreate(Class, BiConsumer)} callback hooks:
 *
 * <pre class="code">
 * connection.registerOnCreate(MockParameterizedRowCountOperation.class, (sql, operation) -> {
 * operation.setRowCount(100);
 * });</pre>
 *
 * @author Mark Paluch
 */
public class MockSession implements Session {

    private final List<SessionLifecycleListener> listeners = new ArrayList<>();
    private final List<CreationListener<?>> operationCreationListener = new ArrayList<>();
    private final List<Map.Entry<String, Operation<?>>> operations = new ArrayList<>();

    private final Map<SessionProperty, Object> connectionProperties;
    private final MockOperation<Void> attachOperation = new MockOperation<Void>().onSubmit(() -> setConnectionLifecycle(Lifecycle.ATTACHED));
    private final MockOperation<Void> closeOperation = new MockOperation<Void>().onSubmit(() -> setConnectionLifecycle(Lifecycle.CLOSED));
    private final MockOperation<Object> catchOperation = new MockOperation<>();
    private final MockOperation<TransactionOutcome> endTransactionOperation = new MockOperation<>();

    private Lifecycle lifecycle;
    private MockTransaction transaction = new MockTransaction();

    /**
     * Creates a new {@link MockSession}.
     */
    public MockSession() {
        this(Collections.emptyMap());
    }

    /**
     * Creates a new {@link MockSession} given {@code connectionProperties}.
     */
    public MockSession(Map<SessionProperty, Object> connectionProperties) {

        this.connectionProperties = connectionProperties;
        setConnectionLifecycle(Lifecycle.NEW);

        registerOnCreate(it -> true, (sql, operation) -> operations.add(new Map.Entry<>() {

            @Override
            public String getKey() {
                return sql;
            }

            @Override
            public Operation<?> getValue() {
                return operation;
            }

            @Override
            public Operation<?> setValue(Operation<?> value) {
                throw new UnsupportedOperationException();
            }
        }));
    }

    @Override
    public MockOperation<Void> attachOperation() {
        return attachOperation;
    }

    @Override
    public MockOperation<Void> validationOperation(Validation depth) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MockOperation<Void> closeOperation() {
        return closeOperation;
    }

    @Override
    public <S, T> OperationGroup<S, T> operationGroup() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MockSession registerLifecycleListener(SessionLifecycleListener listener) {

        this.listeners.add(listener);
        return this;
    }

    @Override
    public MockSession deregisterLifecycleListener(SessionLifecycleListener listener) {

        this.listeners.remove(listener);
        return this;
    }

    @Override
    public Lifecycle getSessionLifecycle() {
        return lifecycle;
    }

    /**
     * Set the {@link jdk.incubator.sql2.Connection.Lifecycle}.
     *
     * @param lifecycle
     */
    public MockSession setConnectionLifecycle(Lifecycle lifecycle) {

        for (SessionLifecycleListener listener : listeners) {
            listener.lifecycleEvent(this, this.lifecycle, lifecycle);

        }
        this.lifecycle = lifecycle;

        return this;
    }

    @Override
    public MockSession abort() {
        setConnectionLifecycle(Lifecycle.ABORTING);
        return this;
    }

    @Override
    public Map<SessionProperty, Object> getProperties() {
        return connectionProperties;
    }

    @Override
    public ShardingKey.Builder shardingKeyBuilder() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationGroup<Object, Object> parallel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationGroup<Object, Object> independent() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationGroup<Object, Object> conditional(CompletionStage<Boolean> condition) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationGroup<Object, Object> collect(Collector<Object, ?, Object> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MockOperation<Object> catchOperation() {
        return catchOperation;
    }

    @Override
    public <R> ArrayRowCountOperation<R> arrayRowCountOperation(String sql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> MockParameterizedRowCountOperation<R> rowCountOperation(String sql) {
        return newOperation(sql, new MockParameterizedRowCountOperation<>());
    }

    @Override
    public MockOperation<Object> operation(String sql) {
        return newOperation(sql, new MockOperation<>());
    }

    @Override
    public <R> OutOperation<R> outOperation(String sql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> MockParameterizedRowOperation<R> rowOperation(String sql) {
        return newOperation(sql, new MockParameterizedRowOperation<>());
    }

    @Override
    public <R> ParameterizedRowPublisherOperation<R> rowPublisherOperation(String sql) {
        return newOperation(sql, new MockParameterizedRowOperation<>());
    }

    @Override
    public <R> MultiOperation<R> multiOperation(String sql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransactionCompletion transactionCompletion() {
        return transaction;
    }

    @Override
    public Operation<TransactionOutcome> endTransactionOperation(TransactionCompletion trans) {

        endTransactionOperation.completeWith(trans.isRollbackOnly() ? TransactionOutcome.ROLLBACK : TransactionOutcome.COMMIT);

        return endTransactionOperation;
    }

    @Override
    public <R> LocalOperation<R> localOperation() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationGroup<Object, Object> logger(Logger logger) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationGroup<Object, Object> timeout(Duration minTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OperationGroup<Object, Object> onError(Consumer<Throwable> handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MockSubmission<Object> submit() {
        return new MockOperation<>().submit();
    }

    /**
     * Register a {@link BiConsumer listener} that is called whenever a SQL {@link Operation} is created for the given operation {@link Class type}.
     *
     * @param type     filter predicate to filter callbacks for operations that are assignable to {@link Class type}.
     * @param listener callback listener.
     * @return {@literal this} {@link MockSession}.
     */
    public <T extends Operation<?>> MockSession registerOnCreate(Class<? super T> type, BiConsumer<String, T> listener) {
        this.operationCreationListener.add(new CreationListener<>(type::isInstance, it -> true, listener));
        return this;
    }

    /**
     * Register a {@link BiConsumer listener} that is called whenever a SQL {@link Operation} is created matching the given {@link Predicate SQL string predicate}.
     *
     * @param sqlPredicate SQL predicate.
     * @param listener     callback listener.
     * @return {@literal this} {@link MockSession}.
     */
    public <T extends Operation<?>> MockSession registerOnCreate(Predicate<String> sqlPredicate, BiConsumer<String, T> listener) {
        this.operationCreationListener.add(new CreationListener<>(it -> true, sqlPredicate, listener));
        return this;
    }

    @Override
    public Session requestHook(LongConsumer request) {
        return null;
    }

    private <T extends Operation<?>> T newOperation(String sql, T operation) {
        operationCreationListener.forEach(creationListener -> creationListener.notify(sql, operation));
        return operation;
    }

    /**
     * Value object encapsulating callback listeners.
     */
    private static class CreationListener<T extends Operation<?>> {

        private final BiConsumer<String, T> listener;
        private final Predicate<Object> typeCondition;
        private final Predicate<String> sqlCondition;

        CreationListener(Predicate<Object> typeCondition, Predicate<String> sqlCondition, BiConsumer<String, T> listener) {
            this.listener = listener;
            this.typeCondition = typeCondition;
            this.sqlCondition = sqlCondition;
        }

        @SuppressWarnings("unchecked")
        void notify(String sql, Operation<?> operation) {

            if (sqlCondition.test(sql) && typeCondition.test(operation)) {
                listener.accept(sql, (T) operation);
            }
        }
    }
}
