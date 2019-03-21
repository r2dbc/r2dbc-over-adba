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

import jdk.incubator.sql2.Result;
import jdk.incubator.sql2.SqlType;
import reactor.util.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * Mock implementation of {@link jdk.incubator.sql2.Result.RowColumn}.
 * <p/> A {@link jdk.incubator.sql2.Result.RowColumn} represents a single row in a row result that can be traversed for its columns. {@link jdk.incubator.sql2.Result.RowColumn} holds a mutable state regarding the its iteration progress across columns.
 *
 * @author Mark Paluch
 * @see ResultBuilder
 */
public class MockRowColumn implements Result.RowColumn {

    private final long rowNumber;
    private final List<MockColumn> columns;

    private boolean canceled = false;
    private int currentIndex;

    /**
     * Create a new {@link MockRowColumn} given its {@code rowNumber}.
     *
     * @param rowNumber
     */
    public MockRowColumn(long rowNumber) {
        this.rowNumber = rowNumber;
        this.columns = new ArrayList<>();
    }

    /**
     * Create a new {@link MockRowColumn} given its {@code rowNumber}, {@link MockColumn columns} and the {@code currentIndex}
     *
     * @param rowNumber
     * @param columns
     */
    public MockRowColumn(long rowNumber, List<MockColumn> columns) {
        this(rowNumber, columns, 0);
    }

    private MockRowColumn(long rowNumber, List<MockColumn> columns, int currentIndex) {
        this.rowNumber = rowNumber;
        this.columns = new ArrayList<>(columns);
        this.currentIndex = currentIndex;
    }

    /**
     * Add a column to this {@link MockRowColumn}. Adding columns leaves the current iteration index untouched.
     *
     * @param column must not be {@literal null}.
     * @return {@literal this} {@link MockRowColumn}.
     */
    public MockRowColumn addColumn(MockColumn column) {

        columns.add(column);
        return this;
    }

    @Override
    public long rowNumber() {
        return rowNumber;
    }

    @Override
    public void cancel() {
        canceled = true;
    }

    /**
     * @return {@literal true} if {@link #cancel()} was called.
     */
    public boolean isCanceled() {
        return canceled;
    }

    @Override
    @Nullable
    public <T> T get(Class<T> type) {
        return current().get(type);
    }

    @Override
    @Nullable
    public String identifier() {
        return current().identifier();
    }

    @Override
    public int index() {
        return this.currentIndex;
    }

    @Override
    public int absoluteIndex() {
        return this.currentIndex;
    }

    @Override
    public SqlType sqlType() {
        return current().sqlType();
    }

    @Override
    public <T> Class<T> javaType() {
        return current().javaType();
    }

    @Override
    public long length() {
        return current().length();
    }

    @Override
    public int numberOfValuesRemaining() {
        return columns.size() - currentIndex;
    }

    @Override
    public Column at(String id) {

        List<MockColumn> columns = this.columns.stream().filter(it -> id.equals(it.identifier())).collect(Collectors.toList());

        if (columns.size() != 1) {
            throw new NoSuchElementException(String.format("Found %d for %s", columns.size(), id));
        }

        this.currentIndex = columns.get(0).index();
        return this;
    }

    @Override
    public Column at(int index) {

        if (index > 0) {
            this.currentIndex = index - 1;
        }
        if (index < 0) {
            if (Math.abs(index) > columns.size()) {
                throw new IndexOutOfBoundsException("Index " + index + " requested, but only " + columns.size() + " available.");
            }

            this.currentIndex = columns.size() + index;
        }
        return this;
    }

    @Override
    public Column slice(int numValues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Column clone() {
        return new MockRowColumn(rowNumber, columns, currentIndex);
    }

    private MockColumn current() {
        return columns.get(index());
    }
}
