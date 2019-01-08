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
package io.r2dbc.adba.mock;

import jdk.incubator.sql2.Operation;
import jdk.incubator.sql2.PrimitiveOperation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Mock implementation of {@link Operation} and {@link PrimitiveOperation}. <p> This {@link Operation} can be customized for a specific outcome or {@link Throwable} to be emitted as result of the {@link #submit() submission}. The operation can be reused across multiple submissions and will create a new {@link MockSubmission} on {@link #submit()}.
 *
 * @author Mark Paluch
 * @see #completeWith(Object)
 * @see #completeWithError(Throwable)
 */
public class MockOperation<T> implements Operation<T>, PrimitiveOperation<T> {

    private final List<Runnable> onSubmit = new ArrayList<>();

    private Consumer<Throwable> handler = t -> {
    };
    private Duration timeout = Duration.ZERO;
    Throwable throwableToThrow;
    T outcome;

    @Override
    public MockOperation<T> onError(Consumer<Throwable> handler) {

        this.handler = handler;
        return this;
    }

    @Override
    public MockOperation<T> timeout(Duration minTime) {

        this.timeout = minTime;
        return this;
    }

    /**
     * @return the {@link Duration timeout} configured via {@link #timeout(Duration)}.
     */
    public Duration getTimeout() {
        return timeout;
    }

    /**
     * Setup stubbing for a successful outcome of this {@link Operation} emitting {@code outcome} on completion.
     *
     * @param outcome
     * @return {@literal this} {@link MockOperation}.
     */
    public MockOperation<T> completeWith(T outcome) {

        this.outcome = outcome;
        this.throwableToThrow = null;

        return this;
    }

    /**
     * Setup stubbing for an exception outcome of this {@link Operation} emitting {@link Throwable} on completion.
     *
     * @param throwable
     * @return {@literal this} {@link MockOperation}.
     */
    public MockOperation<T> completeWithError(Throwable throwable) {

        this.outcome = null;
        this.throwableToThrow = throwable;

        return this;
    }

    /**
     * Register a {@link callback-hook} that is invoked on submission.
     *
     * @param runnable
     * @return {@literal this} {@link MockOperation}.
     */
    public MockOperation<T> onSubmit(Runnable runnable) {

        this.onSubmit.add(runnable);

        return this;
    }

    @Override
    public MockSubmission<T> submit() {

        CompletableFuture<T> result = new CompletableFuture<>();
        if (throwableToThrow != null) {
            result.completeExceptionally(throwableToThrow);
            handler.accept(throwableToThrow);
        } else {
            result.complete(outcome);
        }

        onSubmit.forEach(Runnable::run);

        return new MockSubmission<>(result);
    }
}
