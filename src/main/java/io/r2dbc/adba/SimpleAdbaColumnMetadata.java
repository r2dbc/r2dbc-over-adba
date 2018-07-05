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

import io.r2dbc.spi.ColumnMetadata;
import jdk.incubator.sql2.Result;
import reactor.util.annotation.Nullable;

import java.util.Optional;

/**
 * Simple implementation of {@link ColumnMetadata}.
 *
 * @author Mark Paluch
 */
class SimpleAdbaColumnMetadata implements AdbaColumnMetadata {

    private final @Nullable
    String name;
    private final int index;
    private final Optional<Integer> precision;
    private final Integer type;

    private SimpleAdbaColumnMetadata(String name, int index, Optional<Integer> precision, @Nullable Integer type) {
        this.name = name;
        this.index = index;
        this.precision = precision;
        this.type = type;
    }

    /**
     * Create {@link SimpleAdbaColumnMetadata} from {@link jdk.incubator.sql2.Result.Column}.
     *
     * @param column must not be {@literal null}.
     * @return {@link SimpleAdbaColumnMetadata} for {@link jdk.incubator.sql2.Result.Column}.
     */
    static SimpleAdbaColumnMetadata from(Result.Column column) {
        return new SimpleAdbaColumnMetadata(column.identifier(), column.index(), Optional.empty(), null);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Optional<Integer> getPrecision() {
        return precision;
    }

    @Override
    public Integer getType() {
        return type;
    }

    @Override
    public int getIndex() {
        return index;
    }
}
