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

import io.r2dbc.adba.mock.MockDataSource;
import io.r2dbc.adba.mock.MockSession;
import jdk.incubator.sql2.Session;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link io.r2dbc.spi.ConnectionFactory} using {@link io.r2dbc.adba.mock.MockDataSource}.
 *
 * @author Mark Paluch
 */
class MockSessionFactoryTests {

    @Test
    void shouldConnectSuccessfully() {


        MockDataSource dataSource = MockDataSource.newSingletonMock();
        MockSession connection = dataSource.getSession();

        StepVerifier.create(AdbaAdapter.fromDataSource(dataSource).create()).expectNextCount(1).verifyComplete();

        assertThat(connection.getSessionLifecycle()).isEqualTo(Session.Lifecycle.ATTACHED);
    }
}
