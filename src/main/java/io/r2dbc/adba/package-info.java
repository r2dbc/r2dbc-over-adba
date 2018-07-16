/**
 * An implementation of the Reactive Relational Database Connection API over ADBA-based database drivers. This package contains
 * {@link io.r2dbc.adba.AdbaAdapter} as entry-point and supporting implementation classes to use
 * ADBA implementations through R2DBC's {@link io.r2dbc.spi.ConnectionFactory}.
 *
 * <pre class="code">
 * jdk.incubator.sql2.DataSource dataSource = …;
 * ConnectionFactory connectionFactory = AdbaAdapter.fromDataSource(dataSource);
 * </pre>
 */
@reactor.util.annotation.NonNullApi
package io.r2dbc.adba;
