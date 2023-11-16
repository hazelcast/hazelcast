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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JoinPredicateScanResultSetIteratorTest {

    private static final H2DatabaseProvider h2DatabaseProvider = new H2DatabaseProvider();

    private static String dbConnectionUrl;

    private static final List<String> EXPECTED_TABLES = List.of("VIEWS", "TABLES", "ROLES", "USERS");

    @BeforeAll
    public static void beforeAll() {
        dbConnectionUrl = h2DatabaseProvider.createDatabase(JoinPredicateScanResultSetIteratorTest.class.getName());
    }

    @AfterAll
    public static void afterAll() {
        h2DatabaseProvider.shutdown();
    }

    @Test
    void testHasNext() throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbConnectionUrl)) {
            JoinPredicateScanResultSetIterator<String> iterator = createIterator(connection);
            ArrayList<String> tableNameList = new ArrayList<>();
            while (iterator.hasNext()) {
                tableNameList.add(iterator.next());
            }
            assertThat(tableNameList).containsAll(EXPECTED_TABLES);

            // Call hasNext() and next() methods one more time after the loop to test their behavior
            assertThat(iterator.hasNext()).isFalse();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void testNext() throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbConnectionUrl)) {
            JoinPredicateScanResultSetIterator<String> iterator = createIterator(connection);
            ArrayList<String> tableNameList = new ArrayList<>();
            while (true) {
                try {
                    tableNameList.add(iterator.next());
                } catch (NoSuchElementException exception) {
                    break;
                }
            }
            assertThat(tableNameList).containsAll(EXPECTED_TABLES);

            // Call hasNext() and next() methods one more time after the loop to test their behavior
            assertThat(iterator.hasNext()).isFalse();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void testHasNextAndNext_mixed() throws SQLException {
        try (Connection connection = DriverManager.getConnection(dbConnectionUrl)) {
            JoinPredicateScanResultSetIterator<String> iterator = createIterator(connection);
            ArrayList<String> tableNameList = new ArrayList<>();
            for (int index = 0; ; index++) {
                if (isEven(index)) {
                    if (iterator.hasNext()) {
                        tableNameList.add(iterator.next());
                    } else {
                        break;
                    }
                } else {
                    try {
                        tableNameList.add(iterator.next());
                    } catch (NoSuchElementException exception) {
                        break;
                    }
                }
            }
            assertThat(tableNameList).containsAll(EXPECTED_TABLES);

            // Call hasNext() and next() methods one more time after the loop to test their behavior
            assertThat(iterator.hasNext()).isFalse();
            assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
        }
    }

    private boolean isEven(int value) {
        return value % 2 == 0;
    }

    private JoinPredicateScanResultSetIterator<String> createIterator(Connection connection) {
        return new JoinPredicateScanResultSetIterator<>(connection,
                "SELECT * FROM INFORMATION_SCHEMA.TABLES",
                this::rowMapper,
                this::preparedStatementSetter
        );
    }

    private void preparedStatementSetter(PreparedStatement ignored) {
        // Do nothing with the PreparedStatement
    }

    private String rowMapper(ResultSet resultSet) {
        try {
            String tableName = null;
            if (resultSet.next()) {
                tableName = resultSet.getString("TABLE_NAME");
            }
            return tableName;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
