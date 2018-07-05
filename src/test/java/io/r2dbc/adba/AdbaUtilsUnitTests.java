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

import io.r2dbc.spi.R2dbcException;
import jdk.incubator.sql2.Operation;
import jdk.incubator.sql2.SqlException;
import jdk.incubator.sql2.Submission;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link AdbaUtils}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings("unchecked")
class AdbaUtilsUnitTests {

    @Test
    void submitLaterShouldDeferExecution() {

        Operation<Object> operation = mock(Operation.class);

        AdbaUtils.submitLater(() -> operation);

        verifyZeroInteractions(operation);
    }

    @Test
    void submitLaterShouldSubmitOperation() {

        Operation<Object> operation = mock(Operation.class);
        Submission<Object> submission = mock(Submission.class);

        doReturn(submission).when(operation).submit();
        doReturn(CompletableFuture.completedFuture(new Object())).when(submission).getCompletionStage();

        Mono<Object> mono = AdbaUtils.submitLater(() -> operation);

        mono.as(StepVerifier::create).expectNextCount(1).verifyComplete();

        verify(operation).submit();
    }

    @Test
    void executeLaterShouldDeferExecution() {

        AdbaUtils.executeLater(() -> {
            throw new RuntimeException();
        });
    }

    @Test
    void executeLaterShouldObtainFuture() {

        Supplier<CompletionStage<Object>> supplier = mock(Supplier.class);
        when(supplier.get()).thenReturn(CompletableFuture.completedFuture(new Object()));

        Mono<Object> mono = AdbaUtils.executeLater(supplier);

        mono.as(StepVerifier::create).expectNextCount(1).verifyComplete();
    }

    @Test
    void shouldMapGenericExceptions() {

        NullPointerException cause = new NullPointerException();
        R2dbcException result = AdbaUtils.exceptionMapper().apply(cause);

        assertThat(result).hasCause(cause);
    }

    @Test
    void shouldMapSqlExceptions() {

        SqlException cause = new SqlException("foo", null, "state", 42, "sql", 2);

        AdbaException result = AdbaUtils.exceptionMapper().apply(cause);

        assertThat(result).hasCause(cause);
        assertThat(result.getMessage()).isEqualTo("foo");
        assertThat(result.getErrorCode()).isEqualTo(42);
        assertThat(result.getSqlState()).isEqualTo("state");
        assertThat(result.getSqlString()).isEqualTo("sql");
        assertThat(result.getPosition()).isEqualTo(2);
    }
}
