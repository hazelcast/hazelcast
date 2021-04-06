/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlLimitTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void limitOverTable() {
        String tableName = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertContainsOnlyOneOfRows(
                "SELECT name FROM " + tableName + " LIMIT 1",
                asList(new Row("Alice"), new Row("Bob"), new Row("Joey"))
        );

        assertContainsSubsetOfRows(
                "SELECT name FROM " + tableName + " LIMIT 2",
                2,
                asList(new Row("Alice"), new Row("Bob"), new Row("Joey"))
        );

        assertRowsAnyOrder(
                "SELECT name FROM " + tableName + " LIMIT 5",
                asList(new Row("Alice"), new Row("Bob"), new Row("Joey"))
        );
    }

    @Test
    public void negativeLimitValue() {
        String tableName = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        checkFailure0("SELECT name FROM " + tableName + " LIMIT -10", SqlErrorCode.PARSING, "Encountered \"-\"");
    }

    @Test
    public void floatNumber_asLimitValue() {
        String tableName = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name FROM " + tableName + " LIMIT 5.2",
                asList(new Row("Alice"), new Row("Bob"), new Row("Joey"))
        );
    }

    private static String createTable(String[]... values) {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(QueryDataType.VARCHAR, QueryDataType.INT),
                asList(values)
        );
        return name;
    }

    @Test
    public void limitOverStream() {
        assertContainsOnlyOneOfRows(
                "SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT 1",
                Collections.singletonList(new Row(0L))
        );

        assertContainsSubsetOfRows(
                "SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT 2",
                2,
                asList(new Row(0L),
                new Row(1L))
        );

        assertContainsSubsetOfRows(
                "SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT 10",
                10,
                asList(new Row(0L),
                new Row(1L),
                new Row(2L),
                new Row(3L),
                new Row(4L),
                new Row(5L),
                new Row(6L),
                new Row(7L),
                new Row(8L),
                new Row(9L))
        );
    }

    protected void checkFailure0(
            String sql,
            int expectedErrorCode,
            String expectedErrorMessage,
            Object... params
    ) {
        try {
            SqlStatement statement = new SqlStatement(sql);
            statement.setParameters(asList(params));
            sqlService.execute(statement);

            fail("Must fail");
        } catch (HazelcastSqlException e) {
            assertTrue(expectedErrorMessage.length() != 0);
            assertNotNull(e.getMessage());
            assertTrue(
                    "\nExpected: " + expectedErrorMessage + "\nActual: " + e.getMessage(),
                    e.getMessage().contains(expectedErrorMessage)
            );

            assertEquals(e.getCode() + ": " + e.getMessage(), expectedErrorCode, e.getCode());
        }
    }

    private static void assertContainsOnlyOneOfRows(String sql, Collection<Row> expectedRows) {
        assertContainsSubsetOfRows(sql, 1, expectedRows);
    }

    /**
     * Asserts that the result of {@code sql} contains a subset of {@code expectedRows}, but
     * only a subset of them with size of {@code subsetSize}.
     */
    private  static void assertContainsSubsetOfRows(String sql, int subsetSize, Collection<Row> expectedRows) {
        SqlService sqlService = instance().getSql();
        List<Row> actualRows = new ArrayList<>();
        sqlService.execute(sql).iterator().forEachRemaining(sqlRow -> {
            int columnCount = sqlRow.getMetadata().getColumnCount();
            Object[] values = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                values[i] = sqlRow.getObject(i);
            }
            actualRows.add(new Row(values));
        });
        assertThat(actualRows).hasSize(subsetSize).containsAnyOf(expectedRows.toArray(new Row[subsetSize]));
    }
}
