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

import jdk.incubator.sql2.SqlType;

import java.util.*;

/**
 * Builder for a operation result emitting {@link jdk.incubator.sql2.Result.RowColumn}.
 *
 * @author Mark Paluch
 */
public class ResultBuilder {

    private final Map<String, SqlType> columnDefs;
    private final List<List<Object>> values = new ArrayList<>();

    private ResultBuilder(Map<String, SqlType> columnDefs) {
        this.columnDefs = new LinkedHashMap<>(columnDefs);
    }

    /**
     * Create a new {@link ColumnDefBuilder} to build a result set.
     *
     * @return a new {@link ColumnDefBuilder}.
     */
    public static ColumnDefBuilder builder() {
        return new ColumnDefBuilder();
    }

    /**
     * Configure a new row containing {@code values}.
     *
     * @param values the values for the row. Value count must match the previously configured column count. May contain {@literal null} values.
     * @return {@literal this} {@link ResultBuilder}.
     */
    public ResultBuilder withRow(Object... values) {

        if (columnDefs.size() != values.length) {
            throw new IllegalArgumentException(String.format("Value count %d does not match column count %d!", values.length, columnDefs.size()));
        }

        this.values.add(Arrays.asList(values));
        return this;
    }

    public List<MockRowColumn> build() {


        List<MockRowColumn> result = new ArrayList<>();

        int rowNum = 0;
        for (List<Object> rowValues : values) {

            int index = 0;
            List<MockColumn> columns = new ArrayList<>();
            for (Map.Entry<String, SqlType> entry : columnDefs.entrySet()) {
                MockColumn column = new MockColumn(entry.getKey(), index, rowValues.get(index), entry.getValue());

                index++;

                columns.add(column);

            }
            result.add(new MockRowColumn(rowNum++, columns));
        }

        return result;
    }

    /**
     * Builder to configure {@link jdk.incubator.sql2.Result.Column columns} of the result.
     */
    public static class ColumnDefBuilder {

        private Map<String, SqlType> columnDefs = new LinkedHashMap<>();

        private ColumnDefBuilder() {
        }

        /**
         * Configure a new column.
         *
         * @param name
         * @param type
         * @return {@literal this} {@link ColumnDefBuilder}.
         */
        public ColumnDefBuilder withColumn(String name, SqlType type) {

            columnDefs.put(name, type);
            return this;
        }

        /**
         * @return the {@link ResultBuilder}.
         */
        public ResultBuilder andResult() {
            return new ResultBuilder(columnDefs);
        }
    }
}
