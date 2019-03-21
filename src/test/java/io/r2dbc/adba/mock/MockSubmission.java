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

import jdk.incubator.sql2.Submission;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Mock implementation of {@link Submission}.
 *
 * @author Mark Paluch
 */
public class MockSubmission<T> implements Submission<T> {

    private boolean canceled = false;
    private CompletionStage<T> completionStage;

    public MockSubmission(CompletionStage<T> completionStage) {
        this.completionStage = completionStage;
    }

    @Override
    public CompletionStage<Boolean> cancel() {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletionStage<T> getCompletionStage() {
        return completionStage;
    }

    /**
     * @return {@literal true} if {@link #cancel()} was called.
     */
    public boolean isCanceled() {
        return canceled;
    }
}
