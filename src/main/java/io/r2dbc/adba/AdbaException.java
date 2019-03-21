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
package io.r2dbc.adba;

import io.r2dbc.spi.R2dbcException;
import reactor.util.annotation.Nullable;

/**
 * An exception that represents an ADBA error. This exception is a direct translation of the
 * {@link jdk.incubator.sql2.SqlException}.
 *
 * @author Mark Paluch
 */
public final class AdbaException extends R2dbcException {

    private @Nullable
    final String sqlString;
    private final int position;

    /**
     * Create a new {@link AdbaException}.
     *
     * @param reason    exception message.
     * @param sqlState  vendor-specific SQL state.
     * @param errorCode vendor-specific error code.
     * @param sqlString causing SQL string.
     * @param position  position within the causing SQL string.
     * @param cause     the cause.
     */
    public AdbaException(@Nullable String reason, @Nullable String sqlState, int errorCode, @Nullable String sqlString,
                         int position, @Nullable Throwable cause) {

        super(reason, sqlState, errorCode, cause);

        this.sqlString = sqlString;
        this.position = position;
    }

    /**
     * Create a new {@link AdbaException}.
     *
     * @param cause the cause.
     */
    public AdbaException(Throwable cause) {

        super(cause);

        this.sqlString = null;
        this.position = -1;
    }

    /**
     * The causing SQL string.
     *
     * @return causing SQL string.
     */
    @Nullable
    public String getSqlString() {
        return sqlString;
    }

    /**
     * The position within the causing SQL string.
     *
     * @return position within the causing SQL string.
     */
    public int getPosition() {
        return position;
    }

    @Override
    public String toString() {
        String s = getClass().getName();
        String message = getLocalizedMessage();
        return (message != null) ? (s + ": " + message) : s;
    }

    @Override
    public String getLocalizedMessage() {

        String message = super.getLocalizedMessage();
        return String.format("%s; SQLSTATE=%s; ERROR=%d; SQL=%s; POSITION=%d", message, getSqlState(), getErrorCode(), getSqlString(), getPosition());
    }
}
