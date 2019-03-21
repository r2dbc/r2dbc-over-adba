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

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import jdk.incubator.sql2.DataSource;

import java.util.function.BiFunction;

/**
 * This class is the entry-point ADBA implementations through R2DBC's {@link ConnectionFactory}.
 *
 * <pre class="code">
 * jdk.incubator.sql2.DataSource dataSource = …;
 * ConnectionFactory connectionFactory = AdbaAdapter.fromDataSource(dataSource);
 * </pre>
 *
 * <h3>State</h3> R2DBC supports a broader feature-set than ADBA which leaves certain operations unsupported.
 * <p>
 * Supported operations:
 * <ul>
 * <li>{@link jdk.incubator.sql2.RowCountOperation} through {@link Result#getRowsUpdated()}</li>
 * <li>{@link jdk.incubator.sql2.ParameterizedRowOperation} through {@link Result#map(BiFunction)}</li>
 * <li>Subset of {@link io.r2dbc.spi.RowMetadata} based on result set column identifiers</li>
 * </ul>
 * Unsupported operations:
 * <ul>
 * <li>Savepoints</li>
 * <li>Setting of Transaction Mutability</li>
 * <li>Setting of Transaction Isolation Levels</li>
 * </ul>
 *
 * @author Mark Paluch
 * @see jdk.incubator.sql2.DataSource
 */
public final class AdbaAdapter {

    /**
     * Create a {@link ConnectionFactory} given an {@link DataSource ADBA DataSource}.
     *
     * @param dataSource must not be {@literal null}.
     * @return the {@link ConnectionFactory} adapter for {@link DataSource}.
     */
    public static ConnectionFactory fromDataSource(DataSource dataSource) {

        Assert.notNull(dataSource, "DataSource must not be null!");
        return AdbaConnectionFactory.create(dataSource);
    }
}
