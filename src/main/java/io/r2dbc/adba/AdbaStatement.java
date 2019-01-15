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

package io.r2dbc.adba;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import jdk.incubator.sql2.ParameterizedRowCountOperation;
import jdk.incubator.sql2.ParameterizedRowPublisherOperation;
import jdk.incubator.sql2.Session;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.function.BiFunction;

import static jdk.incubator.sql2.Result.RowColumn;
import static jdk.incubator.sql2.Result.RowCount;

/**
 * R2DBC wrapper for a {@link jdk.incubator.sql2.Session ADBA Connection}. Statements are executed late and lazily on
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

    private final Bindings bindings = new Bindings();

    private final jdk.incubator.sql2.Session session;

    private final String sql;

    private AdbaStatement(Session session, String sql) {

        this.session = session;
        this.sql = sql;
    }

    @Override
    public AdbaStatement add() {
        this.bindings.finish();
        return this;
    }

    @Override
    public AdbaStatement bind(int index, boolean value) {
        return bind(index, (Object) value);
    }

    @Override
    public AdbaStatement bind(int index, byte value) {
        return bind(index, (Object) value);
    }

    @Override
    public AdbaStatement bind(int index, char value) {
        return bind(index, (Object) value);
    }

    @Override
    public AdbaStatement bind(int index, double value) {
        return bind(index, (Object) value);
    }

    @Override
    public AdbaStatement bind(int index, float value) {
        return bind(index, (Object) value);
    }

    @Override
    public AdbaStatement bind(int index, int value) {
        return bind(index, (Object) value);
    }

    @Override
    public AdbaStatement bind(int index, long value) {
        return bind(index, (Object) value);
    }

    @Override
    public AdbaStatement bind(int index, short value) {
        return bind(index, (Object) value);
    }

    @Override
    public AdbaStatement bind(Object identifier, Object value) {

        this.bindings.getCurrent().add((String) identifier, Optional.of(value));
        return this;
    }

    @Override
    public AdbaStatement bind(int index, Object value) {

        this.bindings.getCurrent().add(index, Optional.of(value));
        return this;
    }

    @Override
    public AdbaStatement bindNull(Object identifier, Class<?> aClass) {

        this.bindings.getCurrent().add((String) identifier, Optional.empty());

        return this;
    }

    @Override
    public AdbaStatement bindNull(int index, Class<?> aClass) {

        this.bindings.getCurrent().add(index, Optional.empty());

        return this;
    }

    @Override
    public Mono<AdbaResult> execute() {
        return Mono.just(new AdbaResult());
    }

    /**
     * Creates a {@link AdbaStatement} given {@link Session} and {@code sql}.
     *
     * @param session must not be {@literal null}.
     * @param sql     must not be {@literal null}.
     * @return the {@link AdbaStatement} for {@link Connection} and {@code sql}
     */
    static AdbaStatement create(Session session, String sql) {

        Assert.notNull(session, "Session must not be null!");
        Assert.notNull(sql, "SQL must not be null!");

        return new AdbaStatement(session, sql);
    }

    /**
     * R2DBC wrapper for ADBA operations.
     */
    class AdbaResult implements Result {

        @Override
        public Publisher<Integer> getRowsUpdated() {

            return AdbaUtils.submitLater(() -> {


                ParameterizedRowCountOperation<Number> countOperation = session.rowCountOperation(sql);

                return bindings.getCurrent().bind(countOperation).apply(RowCount::getCount);
            }).map(Number::intValue);
        }

        @Override
        public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> f) {

            EmitterProcessor<RowColumn> rowProcessor = EmitterProcessor.create(true);


            return Flux.defer(() -> {

                ParameterizedRowPublisherOperation<Object> publisherOperation = session.rowPublisherOperation(sql);

                ParameterizedRowPublisherOperation<Object> subscribe =
                        bindings.getCurrent().bind(publisherOperation);

                subscribe.subscribe(new FlowSubscriberAdapter<>(rowProcessor), new CompletableFuture<>()).submit();

                return rowProcessor;
            }).<T>handle((rowColumn, sink) -> {

                AdbaRow row = AdbaRow.create(rowColumn);

                try {

                    T mapped = f.apply(row, row);
                    if (mapped == null) {
                        return;
                    }

                    sink.next(mapped);
                } catch (Exception e) {
                    sink.error(e);
                }
            }).onErrorMap(AdbaUtils.exceptionMapper());
        }
    }

    /**
     * Delegates Reactive Streams {@link Subscription} calls to a {@link Flow.Subscription}.
     */
    static class FlowSubscriptionAdapter implements Subscription {

        private final Flow.Subscription subscription;

        FlowSubscriptionAdapter(Flow.Subscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public void request(long n) {
            subscription.request(n);
        }

        @Override
        public void cancel() {
            subscription.cancel();
        }
    }

    /**
     * Delegates {@link Flow.Subscriber} calls to a Reactive Streams {@link Subscriber}.
     *
     * @param <T>
     */
    static class FlowSubscriberAdapter<T> implements Flow.Subscriber<T> {

        private final Subscriber<T> delegate;

        FlowSubscriberAdapter(Subscriber<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            delegate.onSubscribe(new FlowSubscriptionAdapter(subscription));
        }

        @Override
        public void onNext(T item) {
            delegate.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            delegate.onError(throwable);
        }

        @Override
        public void onComplete() {
            delegate.onComplete();
        }
    }
}
