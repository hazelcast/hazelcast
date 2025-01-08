/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.test.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OracleDatabaseProviderFactoryTest {

    @DisplayName("Parameterized test for Oracle docker images")
    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("testData")
    void testCreateTestDatabaseProvider(String dockerImageName, Class<?> expectedClassType) {
        TestDatabaseProvider result = OracleDatabaseProviderFactory.createTestDatabaseProvider(dockerImageName);
        assertTrue(expectedClassType.isInstance(result));
    }

    private static Stream<Arguments> testData() {
        return Stream.of(
                Arguments.of("gvenzl/oracle-free:23-slim-faststart", OracleFreeDatabaseProvider.class),
                Arguments.of("gvenzl/oracle-xe:21-slim-faststart", OracleXeDatabaseProvider.class)
        );
    }

    @Test
    void testUnknownDockerImageName() {
        assertThrows(
                IllegalArgumentException.class,
                () -> OracleDatabaseProviderFactory.createTestDatabaseProvider("foo"));
    }
}
