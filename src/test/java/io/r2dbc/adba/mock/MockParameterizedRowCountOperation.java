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
package io.r2dbc.adba.mock;

import jdk.incubator.sql2.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Mock implementation of {@link ParameterizedRowOperation} allowing to complete with {@link #setRowCount(long) a row count}.
 *
 * @author Mark Paluch
 */
public class MockParameterizedRowCountOperation<T> extends SqlAwareMockOperation<T> implements ParameterizedRowCountOperation<T> {

    private final Parameters parameters = new Parameters();
    private final List<String> returning = new ArrayList<>();
    private final List<Result.RowColumn> rowColumns = new ArrayList<>();
    private Long rowCount;

    @Override
    @SuppressWarnings("unchecked")
    public MockParameterizedRowCountOperation<T> onError(Consumer<Throwable> handler) {
        return (MockParameterizedRowCountOperation) super.onError(handler);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MockParameterizedRowCountOperation<T> timeout(Duration minTime) {
        return (MockParameterizedRowCountOperation) super.timeout(minTime);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MockParameterizedRowCountOperation<T> completeWith(T outcome) {
        return (MockParameterizedRowCountOperation) super.completeWith(outcome);
    }

    /**
     * Setup stubbing for a successful outcome of this {@link Operation} emitting {@link RowColumn RowColumns} on completion.
     *
     * @param rowColumns
     * @return {@literal this} {@link MockParameterizedRowCountOperation}.
     */
    public MockParameterizedRowCountOperation<T> completeWith(Iterable<? extends Result.RowColumn> rowColumns) {

        for (Result.RowColumn rowColumn : rowColumns) {
            this.rowColumns.add(rowColumn);
        }

        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public MockParameterizedRowCountOperation<T> completeWithError(Throwable throwable) {
        return (MockParameterizedRowCountOperation) super.completeWithError(throwable);
    }

    @Override
    @SuppressWarnings("unchecked")
    public MockParameterizedRowCountOperation<T> onSubmit(Runnable runnable) {
        return (MockParameterizedRowCountOperation) super.onSubmit(runnable);
    }

    @Override
    public MockParameterizedRowOperation<T> returning(String... keys) {

        this.returning.addAll(Arrays.asList(keys));
        return new MockParameterizedRowOperation<>();
    }

    @Override
    public MockParameterizedRowCountOperation<T> apply(Function<Result.RowCount, ? extends T> processor) {

        completeWith(processor.apply(() -> rowCount));

        return this;
    }

    /**
     * Set the row count to report.
     *
     * @param rowCount
     */
    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public MockParameterizedRowCountOperation<T> set(String id, Object value, SqlType type) {

        parameters.set(id, value, type);
        return this;
    }

    @Override
    public MockParameterizedRowCountOperation<T> set(String id, CompletionStage<?> source, SqlType type) {
        parameters.set(id, source, type);
        return this;
    }

    @Override
    public MockParameterizedRowCountOperation<T> set(String id, CompletionStage<?> source) {
        parameters.set(id, source);
        return this;
    }

    @Override
    public MockParameterizedRowCountOperation<T> set(String id, Object value) {
        parameters.set(id, value);
        return this;
    }

    /**
     * @return the bound parameters.
     */
    public Parameters getParameters() {
        return parameters;
    }
}
