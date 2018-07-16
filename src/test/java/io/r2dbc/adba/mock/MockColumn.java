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

import jdk.incubator.sql2.SqlType;
import reactor.util.annotation.Nullable;

/**
 * Mock implementation of a column. Column indexes are zero-based and translated to one-based indexes by
 * {@link jdk.incubator.sql2.Result.RowColumn}.
 * <p/>
 * A column represents a single entry in a row result ("field in a particular row"). {@link MockColumn} can be created
 * by {@link ResultBuilder#builder() ResultBuilder} to form an entire result set with column definitions and result
 * values.
 *
 * @author Mark Paluch
 * @see ResultBuilder
 */
public class MockColumn {

    private final String identifier;
    private final int index;
    private final Object value;
    private final @Nullable
    SqlType type;

    /**
     * Create a new {@link MockColumn} given {@code identifier}, {@code index} (zero-based) and its {@code value}.
     *
     * @param identifier must not be {@literal null}.
     * @param index      positional index, zero-based.
     * @param value
     */
    public MockColumn(String identifier, int index, Object value) {
        this(identifier, index, value, null);
    }

    /**
     * Create a new {@link MockColumn} given {@code identifier}, {@code index} (zero-based), its {@code value}, and
     * {@link SqlType}.
     *
     * @param identifier must not be {@literal null}.
     * @param index      positional index, zero-based.
     * @param value
     * @param type
     */
    public MockColumn(String identifier, int index, Object value, @Nullable SqlType type) {

        this.identifier = identifier;
        this.index = index;
        this.value = value;
        this.type = type;
    }

    /**
     * Return the value of this column as an instance of the given type.
     *
     * @param type
     * @return the value of this {@link jdk.incubator.sql2.Result.Column}.
     */
    @Nullable
    public <T> T get(Class<T> type) {
        return type.cast(this.value);
    }

    /**
     * Return the identifier of this {@link jdk.incubator.sql2.Result.Column}. May be {@literal null}.
     *
     * @return the identifier of this {@link jdk.incubator.sql2.Result.Column}. May be {@literal null}.
     */
    @Nullable
    public String identifier() {
        return this.identifier;
    }

    /**
     * Return the 1-based index of this {@link jdk.incubator.sql2.Result.Column}. The returned value is relative to the
     * slice if this {@link jdk.incubator.sql2.Result.Column} is the result of a call to {@code slice()}.
     * {@code col.slice(n).index() == 1}.
     *
     * @return the index of this {@link jdk.incubator.sql2.Result.Column}.
     */
    public int index() {
        return this.index;
    }

    /**
     * Return the SQL type of the value of this {@link jdk.incubator.sql2.Result.Column}.
     *
     * @return the SQL type of this value.
     */
    public SqlType sqlType() {
        return this.type;
    }

    /**
     * Return the Java type that best represents the value of this {@link jdk.incubator.sql2.Result.Column}.
     *
     * @return a {@link Class} that best represents the value of this {@link jdk.incubator.sql2.Result.Column}.
     */
    @SuppressWarnings("unchecked")
    public <T> Class<T> javaType() {
        return (Class) (this.value != null ? this.value.getClass() : Void.class);
    }

    /**
     * The length of the current value if defined.
     *
     * @return
     * @throws UnsupportedOperationException if the length of the current value is undefined.
     */
    public long length() {

        if (this.value instanceof String) {
            return ((String) this.value).length();
        }

        throw new UnsupportedOperationException(String.format("Length for %s not supported!", this.value));
    }
}
