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

import jdk.incubator.sql2.AdbaType;
import jdk.incubator.sql2.ParameterizedOperation;
import jdk.incubator.sql2.SqlClob;
import jdk.incubator.sql2.SqlType;
import reactor.util.annotation.Nullable;

import java.util.*;
import java.util.Map.Entry;

/**
 * Value object holding the parameter binding for a {@link io.r2dbc.spi.Statement}.
 *
 * @author Mark Paluch
 */
class Binding {

    private final static Map<Class<?>, AdbaType> typeMap = new LinkedHashMap<>();

    static {

        for (AdbaType adbaType : AdbaType.values()) {
            typeMap.put(adbaType.getJavaType(), adbaType);
        }

        typeMap.put(Void.class, AdbaType.NULL);
        typeMap.put(String.class, AdbaType.VARCHAR);
        typeMap.put(Object.class, AdbaType.OTHER);
        typeMap.put(Double.class, AdbaType.DOUBLE);
        typeMap.put(SqlClob.class, AdbaType.CLOB);
        typeMap.put(byte[].class, AdbaType.BINARY);
    }

    private final SortedMap<Integer, Optional<Object>> byIndex = new TreeMap<>();
    private final Map<String, Optional<Object>> byName = new LinkedHashMap<>();

    /**
     * Add a name-based parameter binding.
     *
     * @param identifier name of the parameter to bind, must not be {@literal null} or empty.
     * @param value      the value. Can be {@link Optional#empty()} to bind a {@literal null} value. Must not be
     *                   {@literal null}.
     */
    void add(String identifier, Optional<Object> value) {

        Assert.hasText(identifier, "Identifier must not be empty!");
        Assert.notNull(value, "Optional must not be null!");

        byName.put(identifier, value);
    }

    /**
     * Add a index-based parameter binding. Index-based binding begins at position {@literal 0} for the first parameter to
     * bind (zero-based index).
     *
     * @param index index of the parameter to bind.
     * @param value the value. Can be {@link Optional#empty()} to bind a {@literal null} value. Must not be
     *              {@literal null}.
     */
    void add(int index, Optional<Object> value) {

        Assert.isTrue(index >= 0, "Index must be greater or equal to zero!");
        Assert.notNull(value, "Optional must not be null!");

        byIndex.put(index, value);
    }

    /**
     * Bind registered parameters to a {@link ParameterizedOperation}.
     *
     * @param bindTo the bind target, must not be {@literal null}.
     * @return the bound {@link ParameterizedOperation}.
     */
    @SuppressWarnings("unchecked")
    <T extends ParameterizedOperation<?>> T bind(T bindTo) {

        T bound = bindTo;
        for (Entry<String, Optional<Object>> entry : byName.entrySet()) {

            Optional<Object> value = entry.getValue();

            String key = entry.getKey();
            Object valueToBind = value.orElse(null);
            bound = (T) bound.set(key, valueToBind, determineType(valueToBind));
        }

        for (Entry<Integer, Optional<Object>> entry : byIndex.entrySet()) {

            Optional<Object> value = entry.getValue();

            String key = entry.getKey().toString();
            Object valueToBind = value.orElse(null);
            bound = (T) bound.set(key, valueToBind, determineType(valueToBind));
        }

        return bound;
    }

    static SqlType determineType(@Nullable Object valueToBind) {

        if (valueToBind == null) {
            return AdbaType.NULL;
        }

        for (Entry<Class<?>, AdbaType> entry : typeMap.entrySet()) {
            if (entry.getKey().equals(valueToBind.getClass())) {
                return entry.getValue();
            }
        }

        for (Entry<Class<?>, AdbaType> entry : typeMap.entrySet()) {
            if (entry.getKey() != Object.class && entry.getKey().isInstance(valueToBind)) {
                return entry.getValue();
            }
        }

        return AdbaType.OTHER;
    }
}
