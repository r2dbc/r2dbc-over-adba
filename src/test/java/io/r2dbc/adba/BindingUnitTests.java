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

import jdk.incubator.sql2.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link Binding}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class BindingUnitTests {

    @Mock(answer = Answers.RETURNS_MOCKS)
    ParameterizedOperation<String> operation;

    @Test
    void shouldBindByName() {

        Binding binding = new Binding();
        binding.add("foo", Optional.of("bar"));

        binding.bind(operation);

        verify(operation).set("foo", "bar", AdbaType.VARCHAR);
    }

    @Test
    void shouldBindByIndex() {

        Binding binding = new Binding();
        binding.add(2, Optional.of("bar"));

        binding.bind(operation);

        verify(operation).set("2", "bar", AdbaType.VARCHAR);
    }

    @Test
    void shouldCorrectlyMapTypes() {

        Map<AdbaType, Object> expectation = new LinkedHashMap<>();

        expectation.put(AdbaType.NULL, null);
        expectation.put(AdbaType.INTEGER, 1);
        expectation.put(AdbaType.BIGINT, 1L);
        expectation.put(AdbaType.REAL, 1F);
        expectation.put(AdbaType.DOUBLE, 1D);
        expectation.put(AdbaType.VARCHAR, "foo");
        expectation.put(AdbaType.CLOB, mock(SqlClob.class));
        expectation.put(AdbaType.BINARY, new byte[0]);
        expectation.put(AdbaType.BOOLEAN, true);
        expectation.put(AdbaType.SMALLINT, Short.valueOf("1"));
        expectation.put(AdbaType.TINYINT, Byte.valueOf("1"));
        expectation.put(AdbaType.TIMESTAMP, LocalDateTime.now());
        expectation.put(AdbaType.DATE, LocalDate.now());
        expectation.put(AdbaType.TIME_WITH_TIME_ZONE, OffsetTime.now());
        expectation.put(AdbaType.TIMESTAMP_WITH_TIME_ZONE, OffsetDateTime.now());
        expectation.put(AdbaType.TIME, LocalTime.now());
        expectation.put(AdbaType.BLOB, mock(SqlBlob.class));
        expectation.put(AdbaType.REF, mock(SqlRef.class));
        expectation.put(AdbaType.OTHER, new Object());

        expectation.forEach((expected, value) -> {
            assertThat(Binding.determineType(value)).describedAs("Type for " + value).isEqualTo(expected);
        });
    }
}
