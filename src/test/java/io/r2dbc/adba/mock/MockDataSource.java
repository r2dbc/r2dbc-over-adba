/*
 * Copyright 2018-2019 the original author or authors.
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
package io.r2dbc.adba.mock;

import jdk.incubator.sql2.DataSource;
import jdk.incubator.sql2.Session;
import jdk.incubator.sql2.SessionProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Mock implementation of {@link DataSource}.
 *
 * @author Mark Paluch
 */
public class MockDataSource implements DataSource {

    private Function<Map<SessionProperty, Object>, MockSession> connectionSupplier;
    private boolean closed;

    /**
     * Creates a new {@link MockDataSource} that returns a new {@link MockSession} on each connection request.
     */
    public MockDataSource() {
        this.connectionSupplier = MockSession::new;
    }

    private MockDataSource(Function<Map<SessionProperty, Object>, MockSession> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    /**
     * Creates a new builder for {@link MockDataSource} that allows customization of the mock behavior.
     *
     * @return a new {@link MockDataSourceBuilder}.
     */
    public static MockDataSourceBuilder newMockBuilder() {
        return new MockDataSourceBuilder();
    }

    /**
     * Creates a new singleton {@link MockDataSource} that returns the same  {@link MockSession} on each connect call.
     *
     * @return a new {@link MockDataSource}.
     */
    public static MockDataSource newSingletonMock() {
        return new MockDataSourceBuilder().singletonConnection().build();
    }

    @Override
    public Session.Builder builder() {
        return new MockSessionBuilder(connectionSupplier);
    }

    @Override
    public MockSession getSession() {
        return connectionSupplier.apply(Collections.emptyMap());
    }

    @Override
    public MockSession getSession(Consumer<Throwable> handler) {
        return connectionSupplier.apply(Collections.emptyMap());
    }

    @Override
    public void close() {
        this.closed = true;
    }

    /**
     * @return {@literal true} if this {@link DataSource} was closed.
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Mock implementation of {@link Connection.Builder}.
     */
    public static class MockSessionBuilder implements Session.Builder {

        private final Map<SessionProperty, Object> connectionProperties = new LinkedHashMap<>();
        private final Function<Map<SessionProperty, Object>, MockSession> connectionSupplier;

        private MockSessionBuilder(Function<Map<SessionProperty, Object>, MockSession> connectionSupplier) {
            this.connectionSupplier = connectionSupplier;
        }

        @Override
        public Session.Builder property(SessionProperty p, Object v) {
            connectionProperties.put(p, v);
            return this;
        }

        @Override
        public MockSession build() {
            return connectionSupplier.apply(new LinkedHashMap<>(connectionProperties));
        }
    }

    /**
     * Builder for a {@link MockDataSource}.
     */
    public static class MockDataSourceBuilder {

        private Function<Map<SessionProperty, Object>, MockSession> connectionSupplier;

        /**
         * Configure the builder to use a singleton connection. Concurrent calls to {@link Connection#connect()} are guaranteed to return the same connection instance.
         *
         * @return {@literal this} {@link MockDataSourceBuilder}.
         */
        public MockDataSourceBuilder singletonConnection() {

            AtomicReference<MockSession> ref = new AtomicReference<>();

            Function<Map<SessionProperty, Object>, MockSession> connectionSupplier = cp -> {

                MockSession mockSession = ref.get();

                if (mockSession == null) {
                    ref.compareAndSet(null, new MockSession());
                }

                return ref.get();

            };

            return withConnectionSupplier(connectionSupplier);
        }

        /**
         * Configure the builder to return the provided {@link MockSession}.
         *
         * @param connection the connection to use, must not be {@literal null}.
         * @return {@literal this} {@link MockDataSourceBuilder}.
         */
        public MockDataSourceBuilder singletonConnection(MockSession connection) {
            return withConnectionSupplier(cp -> connection);
        }

        /**
         * Configure the builder to return the provided {@link MockSession}.
         *
         * @return {@literal this} {@link MockDataSourceBuilder}.
         */
        public MockDataSourceBuilder withConnectionSupplier(Function<Map<SessionProperty, Object>, MockSession> connectionSupplier) {

            this.connectionSupplier = connectionSupplier;
            return this;
        }

        /**
         * Build a new {@link MockDataSource}.
         *
         * @return the new {@link MockDataSource}.
         */
        public MockDataSource build() {
            return new MockDataSource(connectionSupplier);
        }
    }
}
