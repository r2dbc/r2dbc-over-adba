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

/**
 * An implementation of the Reactive Relational Database Connection API over ADBA-based database drivers. This package contains
 * {@link io.r2dbc.adba.AdbaAdapter} as entry-point and supporting implementation classes to use
 * ADBA implementations through R2DBC's {@link io.r2dbc.spi.ConnectionFactory}.
 *
 * <pre class="code">
 * jdk.incubator.sql2.DataSource dataSource = …;
 * ConnectionFactory connectionFactory = AdbaAdapter.fromDataSource(dataSource);
 * </pre>
 *
 * @author Mark Paluch
 */
@reactor.util.annotation.NonNullApi
package io.r2dbc.adba;
