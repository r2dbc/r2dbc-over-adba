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

import jdk.incubator.sql2.Operation;
import jdk.incubator.sql2.SqlType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Value object representing bound parameters.
 *
 * @author Mark Paluch
 * @see ParameterBinding
 */
public class Parameters {

    private final Map<String, ParameterBinding> bindings = new LinkedHashMap<>();

    /**
     * @return the bound parameters.
     */
    public Map<String, ParameterBinding> getBindings() {
        return bindings;
    }

    /**
     * Set a parameter value. The value is captured and should not be modified
     * before the {@link Operation} is completed.
     *
     * @param id    the identifier of the parameter marker to be set
     * @param value the value the parameter is to be set to
     * @param type  the SQL type of the value to send to the database
     */
    public void set(String id, Object value, SqlType type) {
        bindings.put(id, ParameterBinding.create(value, type));
    }

    /**
     * Set a parameter value. Use a default SQL type determined by the type of the
     * value argument. The value is captured and should not be modified before the
     * {@link Operation} is completed.
     *
     * @param id    the identifier of the parameter marker to be set
     * @param value the value the parameter is to be set to
     */
    public void set(String id, Object value) {
        bindings.put(id, ParameterBinding.create(value));
    }

    /**
     * Set a parameter value to be the value of a
     * {@link CompletionStage}. The {@link Operation} will
     * not be executed until the {@link CompletionStage} is
     * completed. This method allows submitting {@link Operation}s that depend on
     * the result of previous {@link Operation}s rather than requiring that the
     * dependent {@link Operation} be submitted only when the previous
     * {@link Operation} completes.
     *
     * @param id     the identifier of the parameter marker to be set
     * @param source the {@link CompletionStage} that provides
     *               the value the parameter is to be set to
     * @param type   the SQL type of the value to send to the database
     */
    public void set(String id, CompletionStage<?> source, SqlType type) {
        bindings.put(id, ParameterBinding.create(source, type));
    }

    /**
     * Set a parameter value to be the future value of a
     * {@link CompletionStage}. The {@link Operation} will
     * not be executed until the {@link CompletionStage} is
     * completed. This method allows submitting {@link Operation}s that depend on
     * the result of previous {@link Operation}s rather than requiring that the
     * dependent {@link Operation} be submitted only when the previous
     * {@link Operation} completes. Use a default SQL type determined by the type
     * of the value of the {@link CompletionStage}
     * argument.
     *
     * @param id     the identifier of the parameter marker to be set
     * @param source the {@link CompletionStage} that
     *               provides the value the parameter is to be set to
     */
    public void set(String id, CompletionStage<?> source) {
        bindings.put(id, ParameterBinding.create(source));
    }
}
