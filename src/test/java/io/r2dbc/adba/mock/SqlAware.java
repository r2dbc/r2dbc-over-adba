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

/**
 * Minimal interface for exposing and setting the SQL behind an operation.
 * <p>
 * Implemented by {@link jdk.dynalink.Operation} objects.
 *
 * @author Mark Paluch
 */
public interface SqlAware {

    /**
     * Set the SQL that is associated with this object.
     *
     * @param sql the SQL string.
     */
    void setSql(String sql);

    /**
     * @return the SQL string associated with this object, can be {@literal null}.
     */
    String getSql();
}
