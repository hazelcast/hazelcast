/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.jdbc.join;

import com.hazelcast.test.jdbc.H2DatabaseProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class FullScanResultSetIteratorTest {

    private static final String EMPTY_RESULT = "EMPTY_RESULT";

    private static final H2DatabaseProvider h2DatabaseProvider = new H2DatabaseProvider();

    private static String dbConnectionUrl;

    @BeforeAll
    public static void beforeAll() {
        dbConnectionUrl = h2DatabaseProvider.createDatabase(FullScanResultSetIteratorTest.class.getName());
    }

    @AfterAll
    public static void afterAll() {
        h2DatabaseProvider.shutdown();
    }

    @Test
    void testEmptyResultSetMapperIsCalled() throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbConnectionUrl)) {
            String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'table_does_not_exist'";
            FullScanResultSetIterator<String> iterator = new FullScanResultSetIterator<>(connection,
                    sql,
                    this::rowMapper,
                    () -> EMPTY_RESULT
            );
            ArrayList<String> tableNameList = new ArrayList<>();
            while (iterator.hasNext()) {
                tableNameList.add(iterator.next());
            }
            assertThat(tableNameList).containsExactly(EMPTY_RESULT);

            // Call hasNext() and next() methods one more time after the loop to test their behavior
            assertThat(iterator.hasNext()).isFalse();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void testEmptyResultSetMapperReturnsNull() throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbConnectionUrl)) {
            String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'table_does_not_exist'";
            FullScanResultSetIterator<String> iterator = new FullScanResultSetIterator<>(connection,
                    sql,
                    this::rowMapper,
                    () -> null
            );
            ArrayList<String> tableNameList = new ArrayList<>();
            while (iterator.hasNext()) {
                tableNameList.add(iterator.next());
            }
            assertThat(tableNameList).isEmpty();

            // Call hasNext() and next() methods one more time after the loop to test their behavior
            assertThat(iterator.hasNext()).isFalse();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void testRowMapperIsCalled() throws SQLException {
        List<String> expectedTables = List.of("VIEWS", "TABLES", "ROLES", "USERS");

        try (Connection connection = DriverManager.getConnection(dbConnectionUrl)) {
            String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";
            FullScanResultSetIterator<String> iterator = createIterator(connection, sql);
            ArrayList<String> tableNameList = new ArrayList<>();
            while (iterator.hasNext()) {
                tableNameList.add(iterator.next());
            }
            assertThat(tableNameList).containsAll(expectedTables);

            // Call hasNext() and next() methods one more time after the loop to test their behavior
            assertThat(iterator.hasNext()).isFalse();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    private FullScanResultSetIterator<String> createIterator(Connection connection, String sql) {
        return new FullScanResultSetIterator<>(connection,
                sql,
                this::rowMapper,
                () -> EMPTY_RESULT
        );
    }


    private String rowMapper(ResultSet resultSet) {
        try {
            return resultSet.getString("TABLE_NAME");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
