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

import jdk.incubator.sql2.*;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.function.Consumer;
import java.util.stream.Collector;

/**
 * Mock implementation of {@link ParameterizedRowOperation} that can complete with a {@link #completeWith(Iterable) row result}.
 *
 * @author Mark Paluch
 */
public class MockParameterizedRowOperation<T> extends SqlAwareMockOperation<T> implements ParameterizedRowOperation<T>, ParameterizedRowPublisherOperation<T> {

    private final Parameters parameters = new Parameters();
    private final List<Result.RowColumn> rowColumns = new ArrayList<>();
    @Nullable
    private Flow.Subscriber<? super Result.RowColumn> subscriber;
    private Long fetchSize;

    @Override
    @SuppressWarnings("unchecked")
    public MockParameterizedRowOperation<T> onError(Consumer<Throwable> handler) {
        return (MockParameterizedRowOperation) super.onError(handler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MockParameterizedRowOperation<T> timeout(Duration minTime) {
        return (MockParameterizedRowOperation) super.timeout(minTime);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MockParameterizedRowOperation<T> completeWith(T outcome) {
        return (MockParameterizedRowOperation) super.completeWith(outcome);
    }

    /**
     * Setup stubbing for a successful outcome of this {@link Operation} emitting {@link RowColumn RowColumns} on completion.
     *
     * @param rowColumns
     * @return {@literal this} {@link MockParameterizedRowOperation}.
     */
    public MockParameterizedRowOperation<T> completeWith(Iterable<? extends Result.RowColumn> rowColumns) {


        for (Result.RowColumn rowColumn : rowColumns) {
            this.rowColumns.add(rowColumn);
        }

        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public MockParameterizedRowOperation<T> completeWithError(Throwable throwable) {
        return (MockParameterizedRowOperation) super.completeWithError(throwable);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MockParameterizedRowOperation<T> onSubmit(Runnable runnable) {
        return (MockParameterizedRowOperation) super.onSubmit(runnable);
    }

    @Override
    public MockParameterizedRowOperation<T> fetchSize(long rows) throws IllegalArgumentException {

        this.fetchSize = rows;
        return this;
    }

    @Override
    public MockSubmission<T> submit() {

        if (subscriber != null) {

            SubmissionPublisher<Result.RowColumn> publisher = new SubmissionPublisher<>();
            publisher.subscribe(subscriber);


            if (throwableToThrow != null) {
                publisher.closeExceptionally(throwableToThrow);
            } else {
                rowColumns.forEach(publisher::submit);
                publisher.close();
            }
        }

        return super.submit();
    }

    /**
     * @return the configured fetch size.
     */
    public Long getFetchSize() {
        return this.fetchSize;
    }

    @Override
    public ParameterizedRowPublisherOperation<T> subscribe(Flow.Subscriber<? super Result.RowColumn> subscriber, CompletionStage<? extends T> result) {

        this.subscriber = subscriber;
        return this;
    }

    @Override
    public <A, S extends T> MockParameterizedRowOperation<T> collect(Collector<? super Result.RowColumn, A, S> c) {

        super.completeWith(this.rowColumns.stream().collect(c));
        return this;
    }

    @Override
    public MockParameterizedRowOperation<T> set(String id, Object value, SqlType type) {

        this.parameters.set(id, value, type);
        return this;
    }

    @Override
    public MockParameterizedRowOperation<T> set(String id, CompletionStage<?> source, SqlType type) {
        this.parameters.set(id, source, type);
        return this;
    }

    @Override
    public MockParameterizedRowOperation<T> set(String id, CompletionStage<?> source) {
        this.parameters.set(id, source);
        return this;
    }

    @Override
    public MockParameterizedRowOperation<T> set(String id, Object value) {
        this.parameters.set(id, value);
        return this;
    }

    /**
     * @return the bound parameters.
     */
    public Parameters getParameters() {
        return this.parameters;
    }
}
