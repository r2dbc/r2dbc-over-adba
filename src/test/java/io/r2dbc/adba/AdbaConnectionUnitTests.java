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

import io.r2dbc.adba.mock.MockTransaction;
import io.r2dbc.spi.IsolationLevel;
import jdk.incubator.sql2.Operation;
import jdk.incubator.sql2.Session;
import jdk.incubator.sql2.Submission;
import jdk.incubator.sql2.TransactionOutcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link AdbaConnection}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes"})
class AdbaConnectionUnitTests {

    @Mock
    Session session;
    @Mock
    Operation operation;
    @Mock
    Submission submission;

    AdbaConnection sut;

    @BeforeEach
    void before() {
        sut = AdbaConnection.create(session);
    }

    @Test
    void beginTransaction() {

        sut.beginTransaction().as(StepVerifier::create).verifyComplete();

        verifyZeroInteractions(session);
    }

    @Test
    void commitTransaction() {

        MockTransaction mockTransaction = new MockTransaction();
        when(session.transactionCompletion()).thenReturn(mockTransaction);
        when(session.commitMaybeRollback(mockTransaction)).thenReturn(CompletableFuture.completedFuture(TransactionOutcome.COMMIT));

        sut.commitTransaction().as(StepVerifier::create).verifyComplete();
        verify(session).commitMaybeRollback(mockTransaction);
    }

    @Test
    void rollbackTransaction() {

        when(session.rollback()).thenReturn(CompletableFuture.completedFuture(TransactionOutcome.COMMIT));

        sut.rollbackTransaction().as(StepVerifier::create).verifyComplete();
        verify(session).rollback();
    }

    @Test
    void close() {

        when(session.closeOperation()).thenReturn(operation);
        when(operation.submit()).thenReturn(submission);
        when(submission.getCompletionStage()).thenReturn(CompletableFuture.completedFuture(null));

        sut.close().as(StepVerifier::create).verifyComplete();
        verify(operation).submit();
    }

    @Test
    void createStatement() {

        AdbaStatement statement = sut.createStatement("SELECT * FROM foo");

        assertThat(statement).isNotNull();
    }

    @Test
    void createBatch() {
        assertThatThrownBy(sut::createBatch).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void createSavepoint() {
        assertThatThrownBy(() -> sut.createSavepoint("foo")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void releaseSavepoint() {
        assertThatThrownBy(() -> sut.releaseSavepoint("foo")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rollbackTransactionToSavepoint() {
        assertThatThrownBy(() -> sut.rollbackTransactionToSavepoint("foo")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void setTransactionIsolationLevel() {
        assertThatThrownBy(() -> sut.setTransactionIsolationLevel(IsolationLevel.READ_COMMITTED)).isInstanceOf(UnsupportedOperationException.class);
    }
}
