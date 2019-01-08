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
package io.r2dbc.adba;

import io.r2dbc.adba.mock.*;
import io.r2dbc.spi.Result;
import jdk.incubator.sql2.AdbaType;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for operation execution using {@link MockSession} and operation mocks.
 *
 * @author Mark Paluch
 */
class MockOperationTests {

    @Test
    void shouldExecuteRowOperation() {

        MockDataSource dataSource = MockDataSource.newSingletonMock();
        MockSession session = dataSource.getSession();

        List<MockRowColumn> resultset = ResultBuilder.builder() //
                .withColumn("col", AdbaType.VARCHAR) //
                .andResult() //
                .withRow("foo").withRow("bar") //
                .build();

        session.registerOnCreate(MockParameterizedRowOperation.class, (String sql, MockParameterizedRowOperation<Object> op) -> {
            op.completeWith(resultset);
        });

        Mono<List<String>> result = Mono.from(AdbaAdapter.fromDataSource(dataSource).create()) //
                .flatMapMany(it -> it.createStatement("SELECT * FROM foo").execute())  //
                .flatMap(it -> it.map((r, md) -> r.get("col", String.class))) //
                .collectList();

        result //
                .as(StepVerifier::create) //
                .consumeNextWith(actual -> {
                    assertThat(actual).contains("foo", "bar");
                }).verifyComplete();
    }

    @Test
    void shouldExecuteCountOperation() {

        MockDataSource dataSource = MockDataSource.newSingletonMock();
        MockSession session = dataSource.getSession();

        session.registerOnCreate(MockParameterizedRowCountOperation.class, (sql, op) -> {
            op.setRowCount(100);
        });

        Mono<Integer> result = Mono.from(AdbaAdapter.fromDataSource(dataSource).create()) //
                .flatMapMany(it -> it.createStatement("UPDATE foo").execute()) //
                .flatMap(Result::getRowsUpdated) //
                .next();

        result //
                .as(StepVerifier::create) //
                .expectNext(100).verifyComplete();
    }
}
