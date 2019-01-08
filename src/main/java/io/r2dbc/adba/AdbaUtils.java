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

import jdk.incubator.sql2.Operation;
import jdk.incubator.sql2.SqlException;
import jdk.incubator.sql2.Submission;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utilities for ADBA calls to defer excution and map errors.
 *
 * @author Mark Paluch
 */
class AdbaUtils {

    /**
     * Mapping function to translate ADBA exceptions into R2DBC exceptions.
     */
    private static final Function<Throwable, AdbaException> EXCEPTION_MAPPER = throwable -> {

        if (throwable instanceof SqlException) {
            SqlException ex = (SqlException) throwable;

            return new AdbaException(ex.getMessage(), ex.getSqlState(), ex.getVendorCode(), ex.getSqlString(),
                    ex.getPosition(), ex);
        }

        if (throwable instanceof AdbaException) {
            return (AdbaException) throwable;
        }

        return new AdbaException(throwable);
    };

    /**
     * Create a {@link Mono} that submits an {@link Operation ADBA operation} on subscription.
     *
     * @param operationSupplier the suppler function to obtain a {@link Operation}.
     * @return {@link Mono} wrapper for a {@link Operation} supplier.
     */
    static <T> Mono<T> submitLater(Supplier<Operation<T>> operationSupplier) {

        return Mono.<T>create(it -> {

            Operation<T> operation = operationSupplier.get();
            Submission<T> submission = operation.submit();
            it.onCancel(submission::cancel);
            submission.getCompletionStage().whenComplete((result, e) -> {

                if (e != null) {
                    it.error(e);
                } else {
                    if (result != null) {
                        it.success(result);
                    } else {
                        it.success();
                    }
                }
            });
        }).onErrorMap(exceptionMapper());
    }

    /**
     * Create a {@link Mono} that invokes an asynchronous operation synchronized by {@link CompletionStage} on
     * subscription.
     *
     * @param completionStageSupplier the suppler function to obtain a {@link CompletionStage}.
     * @return {@link Mono} wrapper for a {@link CompletionStage} supplier.
     */
    static <T> Mono<T> executeLater(Supplier<? extends CompletionStage<T>> completionStageSupplier) {
        return Mono.defer(() -> Mono.fromCompletionStage(completionStageSupplier.get())).onErrorMap(exceptionMapper());
    }

    /**
     * Exception mapping {@link Function} that translates {@link SqlException ADBA SqlException} to a
     * {@link AdbaException} using the R2DBC exception hierarchy.
     *
     * @return the exception mapping function.
     */
    static Function<Throwable, AdbaException> exceptionMapper() {
        return EXCEPTION_MAPPER;

    }
}
