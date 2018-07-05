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

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import jdk.incubator.sql2.Connection;
import jdk.incubator.sql2.DataSource;
import jdk.incubator.sql2.Submission;
import reactor.core.publisher.Mono;

/**
 * R2DBC wrapper for a {@link jdk.incubator.sql2.DataSource ADBA DataSource}.
 *
 * @author Mark Paluch
 */
class AdbaConnectionFactory implements ConnectionFactory {

    private final DataSource dataSource;

    /**
     * Creates a new {@link AdbaConnectionFactory} given {@link DataSource}.
     *
     * @param dataSource must not be {@literal null}.
     */
    private AdbaConnectionFactory(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Creates a new {@link AdbaConnectionFactory} given {@link DataSource}.
     *
     * @param dataSource must not be {@literal null}.
     * @return the {@link AdbaConnectionFactory} for {@link DataSource}.
     */
    static AdbaConnectionFactory create(DataSource dataSource) {
        return new AdbaConnectionFactory(dataSource);
    }

    /**
     * Create a {@link Mono} from the {@link jdk.incubator.sql2.Operation ConnectOperation}.
     *
     * @return
     * @see jdk.incubator.sql2.Connection#connectOperation
     */
    @Override
    public Mono<AdbaConnection> create() {

        return Mono.defer(() -> {

            Connection connection = dataSource.builder().build();
            connection.submitHoldingForMoreMembers();
            Submission<Void> submission = connection.connectOperation().submit();

            return Mono.fromCompletionStage(submission.getCompletionStage()).thenReturn(AdbaConnection.create(connection));
        }).onErrorMap(AdbaUtils.exceptionMapper());
    }

    @Override
    public AdbaConnectionFactoryMetadata getMetadata() {
        return AdbaConnectionFactoryMetadata.INSTANCE;
    }

    /**
     * Static {@link ConnectionFactoryMetadata} for the ADBA adapter.
     */
    enum AdbaConnectionFactoryMetadata implements ConnectionFactoryMetadata {

        INSTANCE;

        @Override
        public String getName() {
            return "ADBA Adapter";
        }
    }
}
