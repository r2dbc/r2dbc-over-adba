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

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import jdk.incubator.sql2.Connection;
import jdk.incubator.sql2.ParameterizedRowCountOperation;
import jdk.incubator.sql2.ParameterizedRowOperation;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collector.Characteristics;

/**
 * R2DBC wrapper for a {@link jdk.incubator.sql2.Connection ADBA Connection}. Statements are executed late and lazily on
 * interaction with {@link Result#getRowsUpdated()} or {@link Result#map(BiFunction)} methods. Currently supported
 * operations are:
 * <ul>
 * <li>{@link jdk.incubator.sql2.RowCountOperation}</li>
 * <li>{@link jdk.incubator.sql2.ParameterizedRowOperation}</li>
 * </ul>
 *
 * @author Mark Paluch
 */
class AdbaStatement implements Statement {

    private final jdk.incubator.sql2.Connection connection;
    private final String sql;
    private final Bindings bindings = new Bindings();

    private AdbaStatement(Connection connection, String sql) {

        this.connection = connection;
        this.sql = sql;
    }

    /**
     * Creates a {@link AdbaStatement} given {@link Connection} and {@code sql}.
     *
     * @param connection must not be {@literal null}.
     * @param sql        must not be {@literal null}.
     * @return the {@link AdbaStatement} for {@link Connection} and {@code sql}
     */
    static AdbaStatement create(Connection connection, String sql) {

        Assert.notNull(connection, "Connection must not be null!");
        Assert.notNull(sql, "SQL must not be null!");

        return new AdbaStatement(connection, sql);
    }

    @Override
    public AdbaStatement add() {
        this.bindings.finish();
        return this;
    }

    @Override
    public AdbaStatement bind(Object identifier, Object value) {

        this.bindings.getCurrent().add((String) identifier, Optional.of(value));
        return this;
    }

    @Override
    public AdbaStatement bind(Integer index, Object value) {

        this.bindings.getCurrent().add(index, Optional.of(value));
        return this;
    }

    @Override
    public AdbaStatement bindNull(Object identifier, Object type) {

        if (identifier instanceof Integer) {
            this.bindings.getCurrent().add((Integer) identifier, Optional.empty());
        } else {
            this.bindings.getCurrent().add((String) identifier, Optional.empty());
        }

        return this;
    }

    @Override
    public Mono<AdbaResult> execute() {
        return executeReturningGeneratedKeys();
    }

    @Override
    public Mono<AdbaResult> executeReturningGeneratedKeys() {
        return Mono.just(new AdbaResult());
    }

    /**
     * R2DBC wrapper for ADBA operations.
     */
    private class AdbaResult implements Result {

        @Override
        public Publisher<Integer> getRowsUpdated() {

            return AdbaUtils.submitLater(() -> {

                ParameterizedRowCountOperation<Number> countOperation = connection.rowCountOperation(sql);

                return bindings.getCurrent().bind(countOperation).apply(jdk.incubator.sql2.Result.RowCount::getCount);
            }).map(Number::intValue);
        }

        @Override
        public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {

            Collector<jdk.incubator.sql2.Result.RowColumn, List<T>, List<T>> collector = Collector.of(ArrayList::new,
                    (objects, o) -> {


                        AdbaRow row = AdbaRow.create(o);
                        T mapped = f.apply(row, row);

                        if (mapped == null) {
                            return;
                        }

                        objects.add(mapped);
                    }, (left, right) -> {
                        left.addAll(right);
                        return left;
                    }, it -> it, Characteristics.IDENTITY_FINISH);

            return AdbaUtils.submitLater(() -> {

                ParameterizedRowOperation<List<T>> rowOperation = connection.<List<T>>rowOperation(sql).fetchSize(100)
                        .collect(collector);
                return bindings.getCurrent().bind(rowOperation);
            }).flatMapIterable(Function.identity());
        }
    }
}
