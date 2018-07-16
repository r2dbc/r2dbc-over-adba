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

import io.r2dbc.adba.mock.MockConnection;
import io.r2dbc.adba.mock.MockDataSource;
import jdk.incubator.sql2.Connection;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link io.r2dbc.spi.ConnectionFactory} using {@link io.r2dbc.adba.mock.MockDataSource}.
 *
 * @author Mark Paluch
 */
class MockConnectionFactoryTests {

    @Test
    void shouldConnectSuccessfully() {


        MockDataSource dataSource = MockDataSource.newSingletonMock();
        MockConnection connection = dataSource.getConnection();

        StepVerifier.create(AdbaAdapter.fromDataSource(dataSource).create()).expectNextCount(1).verifyComplete();

        assertThat(connection.getConnectionLifecycle()).isEqualTo(Connection.Lifecycle.OPEN);
    }

    @Test
    void connectShouldFail() {

        MockDataSource dataSource = MockDataSource.newSingletonMock();
        MockConnection connection = dataSource.getConnection();
        connection.connectOperation().completeWithError(new IllegalStateException());

        StepVerifier.create(AdbaAdapter.fromDataSource(dataSource).create()).consumeErrorWith(e -> {
            assertThat(e).isInstanceOf(AdbaException.class).hasCauseInstanceOf(IllegalStateException.class);
        }).verify();
    }
}
