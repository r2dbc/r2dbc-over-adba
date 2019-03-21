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
package io.r2dbc.adba;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import jdk.incubator.sql2.Result;
import reactor.util.annotation.Nullable;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ADBA-specific {@link Row} and {@link RowMetadata} implementation. {@link RowMetadata} is computed lazily when a metadata method is accessed.
 *
 * @author Mark Paluch
 */
class AdbaRow implements Row, RowMetadata {

    private final jdk.incubator.sql2.Result.RowColumn delegate;
    private @Nullable
    Map<Integer, AdbaColumnMetadata> metadataMap;

    /**
     * Creates a new {@link AdbaRow} for {@link jdk.incubator.sql2.Result.RowColumn}.
     *
     * @param delegate must not be {@literal null}.
     */
    private AdbaRow(Result.RowColumn delegate) {
        this.delegate = delegate;
    }

    /**
     * Creates a new {@link AdbaRow} for {@link jdk.incubator.sql2.Result.RowColumn}.
     *
     * @param delegate must not be {@literal null}.
     * @return the {@link AdbaRow} for {@link jdk.incubator.sql2.Result.RowColumn}.
     */
    static AdbaRow create(jdk.incubator.sql2.Result.RowColumn delegate) {

        Assert.notNull(delegate, "RowColumn must not be null!");

        return new AdbaRow(delegate);
    }

    @Override
    public <T> T get(Object identifier, Class<T> type) {
        return doGet(getColumn(identifier), type);
    }

    @Override
    public AdbaColumnMetadata getColumnMetadata(Object identifier) {

        Assert.notNull(identifier, "Identifier must not be null!");

        if (identifier instanceof Integer) {
            return getMetadata().get(identifier);
        }

        return getColumnMetadatas().stream().filter(it -> identifier.equals(it.getName())).findFirst().orElse(null);
    }

    @Override
    public Collection<AdbaColumnMetadata> getColumnMetadatas() {
        return getMetadata().values();
    }

    private Map<Integer, AdbaColumnMetadata> getMetadata() {

        if (metadataMap == null) {
            metadataMap = createMetadataMap();
        }
        return metadataMap;
    }

    private Map<Integer, AdbaColumnMetadata> createMetadataMap() {

        delegate.at(1);

        Map<Integer, AdbaColumnMetadata> metadataMap = new LinkedHashMap<>();

        for (Result.Column column : delegate) {
            metadataMap.put(column.absoluteIndex(), SimpleAdbaColumnMetadata.from(column));
        }


        return metadataMap;
    }

    private Result.Column getColumn(Object identifier) {

        if (identifier instanceof Integer) {

            return delegate.at((Integer) identifier);
        }

        return delegate.at((String) identifier);
    }

    private static <T> T doGet(Result.Column column, @Nullable Class<T> type) {

        if (type != null && !type.equals(Object.class)) {
            return column.get(type);
        }

        return column.get();
    }
}
