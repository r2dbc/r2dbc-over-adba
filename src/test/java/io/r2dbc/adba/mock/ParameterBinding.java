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

import jdk.incubator.sql2.SqlType;
import reactor.util.annotation.Nullable;

/**
 * Value object representing a parameter binding. Used by {@link Parameters}.
 *
 * @author Mark Paluch
 */
public class ParameterBinding {

    private final @Nullable
    Object value;
    private final @Nullable
    SqlType type;

    private ParameterBinding(@Nullable Object value, @Nullable SqlType type) {

        this.value = value;
        this.type = type;
    }

    /**
     * Creates a new {@link ParameterBinding} given {@code value} and its {@link SqlType}.
     *
     * @param value the value. Can be {@literal null}.
     * @param type  the value {@link SqlType type}.
     * @return a new {@link ParameterBinding}.
     */
    public static ParameterBinding create(@Nullable Object value, SqlType type) {
        return new ParameterBinding(value, type);
    }

    /**
     * Creates a new {@link ParameterBinding} given {@code value}.
     *
     * @param value the value. Can be {@literal null}.
     * @return a new {@link ParameterBinding}.
     */
    public static ParameterBinding create(@Nullable Object value) {
        return new ParameterBinding(value, null);
    }

    /**
     * @return the bound value, can be {@literal null}.
     */
    @Nullable
    public Object getValue() {
        return value;
    }

    @Nullable
    public SqlType getType() {
        return type;
    }
}
