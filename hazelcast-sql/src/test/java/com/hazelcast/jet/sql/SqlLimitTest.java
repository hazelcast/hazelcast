/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    public void nullLimitValue() {
        String tableName = createTable(new String[]{"Alice", "1"});

        assertThatThrownBy(() -> sqlService.execute("SELECT name FROM " + tableName + " LIMIT null"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Encountered \"null\"")
                .extracting(e -> ((HazelcastSqlException) e).getCode()).isEqualTo(SqlErrorCode.PARSING);
    }

    @Test
    public void negativeLimitValue() {
        String tableName = createTable(new String[]{"Alice", "1"});

        assertThatThrownBy(() -> sqlService.execute("SELECT name FROM " + tableName + " LIMIT -10"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Encountered \"-\"")
                .extracting(e -> ((HazelcastSqlException) e).getCode()).isEqualTo(SqlErrorCode.PARSING);
    }

    @Test
    public void floatNumber_asLimitValue() {
        String tableName = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertContainsSubsetOfRows(
                "SELECT name FROM " + tableName + " LIMIT 2.99",
                2,
                asList(new Row("Alice"), new Row("Bob"), new Row("Joey"))
        );

        assertContainsSubsetOfRows(
                "SELECT name FROM " + tableName + " LIMIT 2.5",
                2,
                asList(new Row("Alice"), new Row("Bob"), new Row("Joey"))
        );

        assertContainsSubsetOfRows(
                "SELECT name FROM " + tableName + " LIMIT 2.45",
                2,
                asList(new Row("Alice"), new Row("Bob"), new Row("Joey"))
        );

        assertContainsSubsetOfRows(
                "SELECT name FROM " + tableName + " LIMIT 3.1",
                3,
                asList(new Row("Alice"), new Row("Bob"), new Row("Joey"))
        );
    }

    private static String createTable(String[]... values) {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(QueryDataTypeFamily.VARCHAR, QueryDataTypeFamily.INTEGER),
                asList(values)
        );
        return name;
    }

    @Test
    public void limitOverStream() {
        assertRowsOrdered(
                "SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT 1",
                singletonList(new Row(0L))
        );

        assertRowsOrdered(
                "SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT 2",
                asList(new Row(0L),
                new Row(1L))
        );

        assertRowsOrdered(
                "SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT 10",
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

    @Test
    public void limitWithDynamicParameter() {
        assertRowsAnyOrder(
                "SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT ?",
                singletonList(2),
                asList(
                        new Row(0L),
                        new Row(1L)
                )
        );
    }

    @Test
    public void limitWithNullDynamicParameter() {
        SqlStatement statement = new SqlStatement("SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT ?");
        statement.setParameters(singletonList(null));

        assertThatThrownBy(() -> sqlService.execute(statement).iterator().next())
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("LIMIT value cannot be null");
    }

    @Test
    public void limitWithNegativeDynamicParameter() {
        SqlStatement statement = new SqlStatement("SELECT * FROM TABLE(GENERATE_STREAM(5)) LIMIT ?");
        statement.setParameters(singletonList(-1));

        assertThatThrownBy(() -> sqlService.execute(statement).iterator().next())
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("LIMIT value cannot be negative");
    }

    @Test
    public void limitWithProjection() {
        final String sql = "SELECT ABS(v) "
                + "FROM TABLE (generate_series(-10, -1)) "
                + "ORDER BY v ASC "
                + "LIMIT 5";
        assertRowsOrdered(sql, rows(10L, 9L, 8L, 7L, 6L));
    }

    @Test
    public void limitAndOffsetWithProjection() {
        final String sql = "SELECT ABS(v) "
                + "FROM TABLE (generate_series(-10, -1)) "
                + "ORDER BY v ASC "
                + "LIMIT 5 "
                + "OFFSET 2";
        assertRowsOrdered(sql, rows(8L, 7L, 6L, 5L, 4L));
    }

    private static void assertContainsOnlyOneOfRows(String sql, Collection<Row> expectedRows) {
        assertContainsSubsetOfRows(sql, 1, expectedRows);
    }

    /**
     * Asserts that the result of {@code sql} contains a subset of {@code
     * expectedRows}, the subset must be of size {@code subsetSize}.
     */
    private static void assertContainsSubsetOfRows(String sql, int subsetSize, Collection<Row> expectedRows) {
        assertContainsSubsetOfRows(sql, emptyList(), subsetSize, expectedRows);
    }

    /**
     * Asserts that the result of {@code sql} contains a subset of {@code
     * expectedRows}, the subset must be of size {@code subsetSize}.
     */
    private static void assertContainsSubsetOfRows(
            String sql,
            List<Object> arguments,
            int subsetSize,
            Collection<Row> expectedRows
    ) {
        List<Row> actualRows = new ArrayList<>();
        SqlStatement statement = new SqlStatement(sql);
        statement.setParameters(arguments);
        sqlService.execute(statement).iterator().forEachRemaining(sqlRow -> {
            int columnCount = sqlRow.getMetadata().getColumnCount();
            Object[] values = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                values[i] = sqlRow.getObject(i);
            }
            actualRows.add(new Row(values));
        });
        assertThat(actualRows).hasSize(subsetSize).containsAnyOf(expectedRows.toArray(new Row[subsetSize]));
    }

    private static List<Row> rows(Long ...rows) {
        return Stream.of(rows)
                .map(Row::new)
                .collect(Collectors.toList());
    }
}
