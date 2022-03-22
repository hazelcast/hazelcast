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

package com.hazelcast.jet.sql.impl.aggregate;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestStreamSqlConnector;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitParamsRunner.class)
public class SqlAggregateTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_groupBy() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{null, "1"},
                new String[]{"Alice", "1"},
                new String[]{null, "1"}
        );

        assertRowsAnyOrder(
                "SELECT name FROM " + name + " GROUP BY 1", // field ordinal
                asList(
                        new Row("Alice"),
                        new Row("Bob"),
                        new Row((String) null)
                )
        );
        assertRowsAnyOrder(
                "SELECT distance, name FROM " + name + " GROUP BY distance, name",
                asList(
                        new Row(1, "Alice"),
                        new Row(2, "Alice"),
                        new Row(1, "Bob"),
                        new Row(1, null)
                )
        );
    }

    @Test
    public void test_groupByEmpty() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT name FROM " + name + " GROUP BY name",
                emptyList()
        );
    }

    @Test
    public void test_groupByNotSelectedField() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "4"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name FROM " + name + " GROUP BY name, distance",
                asList(
                        new Row("Alice"),
                        new Row("Alice"),
                        new Row("Bob"),
                        new Row("Joey")
                )
        );
    }

    @Test
    public void test_groupByExpression() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, (distance * 2) d FROM " + name + " GROUP BY name, distance * 2",
                asList(
                        new Row("Alice", 2L),
                        new Row("Alice", 4L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_groupByHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, distance FROM " + name + " GROUP BY name, distance HAVING distance < 2",
                asList(
                        new Row("Alice", 1),
                        new Row("Bob", 1)
                )
        );
    }

    @Test
    public void test_groupByExpressionHavingExpression() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "4"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, (distance * 2) d FROM " + name + " GROUP BY name, d HAVING d > 2",
                asList(
                        new Row("Alice", 4L),
                        new Row("Alice", 8L),
                        new Row("Joey", 4L)
                )
        );
    }

    @Test
    public void test_distinct() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "4"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "2"}
        );

        assertRowsAnyOrder(
                "SELECT DISTINCT name FROM " + name,
                asList(
                        new Row("Alice"),
                        new Row("Bob"),
                        new Row("Joey")
                )
        );
    }

    @Test
    public void test_all() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "4"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "2"}
        );

        assertRowsAnyOrder(
                "SELECT ALL name FROM " + name,
                asList(
                        new Row("Alice"),
                        new Row("Alice"),
                        new Row("Alice"),
                        new Row("Bob"),
                        new Row("Joey")
                )
        );
    }

    @Test
    public void test_count() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{null, "1"}
        );

        assertRowsAnyOrder(
                "SELECT COUNT(name) FROM " + name,
                singletonList(new Row(2L))
        );
        assertRowsAnyOrder(
                "SELECT COUNT(*) FROM " + name,
                singletonList(new Row(3L))
        );
    }

    @Test
    public void test_emptyCount() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT COUNT(*) FROM " + name,
                singletonList(new Row(0L))
        );
    }

    @Test
    public void test_emptyCountGroupBy() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT COUNT(*) FROM " + name + " GROUP BY name",
                emptyList()
        );
    }

    @Test
    public void test_distinctCount() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT COUNT(DISTINCT distance) FROM " + name,
                singletonList(new Row(2L))
        );
    }

    @Test
    public void test_groupCount() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, COUNT(*) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 3L),
                        new Row("Bob", 1L)
                )
        );
    }

    @Test
    public void test_groupDistinctCount() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, COUNT(DISTINCT distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 2L),
                        new Row("Bob", 1L)
                )
        );
    }

    @Test
    public void test_groupExpressionCount() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"},
                new String[]{"Joey", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, (distance * 2) d, COUNT(*) FROM " + name + " GROUP BY name, d",
                asList(
                        new Row("Alice", 2L, 2L),
                        new Row("Alice", 4L, 1L),
                        new Row("Bob", 2L, 1L),
                        new Row("Joey", 2L, 1L)
                )
        );
    }

    @Test
    public void test_groupCountExpression() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"},
                new String[]{"Joey", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, 2 * COUNT(*) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 6L),
                        new Row("Bob", 2L),
                        new Row("Joey", 2L)
                )
        );
    }

    @Test
    public void test_groupCountHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, COUNT(*) c FROM " + name + " GROUP BY name, distance HAVING c < 2",
                asList(
                        new Row("Alice", 1L),
                        new Row("Bob", 1L)
                )
        );
    }

    @Test
    public void test_groupCountHavingAggregation() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, COUNT(*) FROM " + name + " GROUP BY name, distance HAVING COUNT(*) < 2",
                asList(
                        new Row("Alice", 1L),
                        new Row("Bob", 1L)
                )
        );
    }

    @Test
    public void test_groupExpressionCountHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, (distance * 2) d, COUNT(*) c FROM " + name + " GROUP BY name, d HAVING c < 2",
                asList(
                        new Row("Alice", 4L, 1L),
                        new Row("Bob", 2L, 1L)
                )
        );
    }

    @Test
    public void test_min() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Joey", null}
        );

        assertRowsAnyOrder(
                "SELECT MIN(name), MIN(distance) FROM " + name,
                singletonList(new Row("Alice", 1))
        );
    }

    @Test
    public void test_emptyMin() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT MIN(name), MIN(distance) FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void test_groupMin() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, MIN(distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 1),
                        new Row("Bob", 2)
                )
        );
    }

    @Test
    public void test_distinctMin() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Joey", null}
        );

        assertRowsAnyOrder(
                "SELECT MIN(DISTINCT name) FROM " + name,
                singletonList(new Row("Alice"))
        );
    }

    @Test
    public void test_groupExpressionMin() {
        String name = createTable(
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, MIN(distance * 2) m FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 2L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_groupMinHaving() {
        String name = createTable(
                new String[]{"Joey", "3"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, MIN(distance) m FROM " + name + " GROUP BY name HAVING m > 1",
                asList(
                        new Row("Bob", 2),
                        new Row("Joey", 3)
                )
        );
    }

    @Test
    public void test_groupExpressionMinHaving() {
        String name = createTable(
                new String[]{"Joey", "3"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, MIN(distance * 2) m FROM " + name + " GROUP BY name HAVING m < 5",
                asList(
                        new Row("Alice", 2L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_max() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", null}
        );

        assertRowsAnyOrder(
                "SELECT MAX(name), MAX(distance) FROM " + name,
                singletonList(new Row("Joey", 2))
        );
    }

    @Test
    public void test_emptyMax() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT MAX(name), MAX(distance) FROM " + name,
                singletonList(new Row(null, null))
        );
    }

    @Test
    public void test_groupMax() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, MAX(distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 2),
                        new Row("Bob", 2)
                )
        );
    }

    @Test
    public void test_distinctMax() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", null}
        );

        assertRowsAnyOrder(
                "SELECT MAX(DISTINCT name) FROM " + name,
                singletonList(new Row("Joey"))
        );
    }

    @Test
    public void test_groupExpressionMax() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, MAX(distance * 2) m FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 4L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_groupMaxHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, MAX(distance) m FROM " + name + " GROUP BY name HAVING m > 1",
                asList(
                        new Row("Alice", 2),
                        new Row("Joey", 3)
                )
        );
    }

    @Test
    public void test_groupExpressionMaxHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, MAX(distance * 2) m FROM " + name + " GROUP BY name HAVING m < 5",
                asList(
                        new Row("Alice", 4L),
                        new Row("Bob", 4L)
                )
        );
    }

    @Test
    public void test_sum() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", null}
        );

        assertRowsAnyOrder(
                "SELECT SUM(distance) FROM " + name,
                singletonList(new Row(3L))
        );
    }

    @Test
    public void test_emptySum() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT SUM(distance) FROM " + name,
                singletonList(new Row((Object) null))
        );
    }

    @Test
    public void test_almostEmptySum() {
        String name = createTable(
                new String[]{"Alice", null}
        );

        assertRowsAnyOrder(
                "SELECT SUM(distance) FROM " + name,
                singletonList(new Row((Object) null))
        );
    }

    @Test
    public void test_distinctSum() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT SUM(DISTINCT distance) FROM " + name,
                singletonList(new Row(3L))
        );
    }

    @Test
    public void test_groupSum() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, SUM(distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 3L),
                        new Row("Bob", 2L)
                )
        );
    }

    @Test
    public void test_groupDistinctSum() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, SUM(DISTINCT distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", 3L),
                        new Row("Bob", 1L)
                )
        );
    }

    @Test
    public void test_groupExpressionSum() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, SUM(distance * 2) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", new BigDecimal(6)),
                        new Row("Bob", new BigDecimal(2))
                )
        );
    }

    @Test
    public void test_groupSumHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, SUM(distance) s FROM " + name + " GROUP BY name HAVING s > 2",
                asList(
                        new Row("Alice", 3L),
                        new Row("Joey", 3L)
                )
        );
    }

    @Test
    public void test_groupExpressionSumHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, SUM(distance * 2) s FROM " + name + " GROUP BY name HAVING s > 4",
                asList(
                        new Row("Alice", new BigDecimal(6)),
                        new Row("Joey", new BigDecimal(6))
                )
        );
    }

    @SuppressWarnings("unused")
    private Object[] sum_types() {
        return new Object[]{
                new Object[]{QueryDataTypeFamily.TINYINT, QueryDataType.BIGINT},
                new Object[]{QueryDataTypeFamily.SMALLINT, QueryDataType.BIGINT},
                new Object[]{QueryDataTypeFamily.INTEGER, QueryDataType.BIGINT},
                new Object[]{QueryDataTypeFamily.BIGINT, QueryDataType.DECIMAL},
                new Object[]{QueryDataTypeFamily.DECIMAL, QueryDataType.DECIMAL},
                new Object[]{QueryDataTypeFamily.REAL, QueryDataType.REAL},
                new Object[]{QueryDataTypeFamily.DOUBLE, QueryDataType.DOUBLE},
        };
    }

    @Test
    @Parameters(method = "sum_types")
    public void test_orderBySum(QueryDataTypeFamily columnType, QueryDataType expectedResultType) {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(VARCHAR, columnType),
                asList(
                        new String[]{"A", "1"},
                        new String[]{"B", "2"},
                        new String[]{"A", "3"},
                        new String[]{"B", "4"}
                )
        );

        assertRowsOrdered(
                "SELECT name, SUM(distance) total_distance FROM " + name + " GROUP BY name ORDER BY total_distance DESC",
                asList(
                        new Row("B", expectedResultType.convert(6)),
                        new Row("A", expectedResultType.convert(4))
                )
        );
    }

    @Test
    public void test_avg() {
        String name = createTable(
                new String[]{"Alice", "4"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", null}
        );

        assertRowsAnyOrder(
                "SELECT AVG(distance) FROM " + name,
                singletonList(new Row(new BigDecimal("3")))
        );
    }

    @Test
    public void test_emptyAvg() {
        String name = createTable();

        assertRowsAnyOrder(
                "SELECT AVG(distance) FROM " + name,
                singletonList(new Row((Object) null))
        );
    }

    @Test
    public void test_distinctAvg() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT AVG(DISTINCT distance) FROM " + name,
                singletonList(new Row(new BigDecimal("1.5")))
        );
    }

    @Test
    public void test_groupAvg() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, AVG(distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", new BigDecimal("1.5")),
                        new Row("Bob", new BigDecimal("2"))
                )
        );
    }

    @Test
    public void test_groupDistinctAvg() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "1"}
        );

        assertRowsAnyOrder(
                "SELECT name, AVG(DISTINCT distance) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", new BigDecimal("1.5")),
                        new Row("Bob", new BigDecimal("1"))
                )
        );
    }

    @Test
    public void test_groupExpressionAvg() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "1"},
                new String[]{"Alice", "2"}
        );

        assertRowsAnyOrder(
                "SELECT name, AVG(distance * 2) FROM " + name + " GROUP BY name",
                asList(
                        new Row("Alice", new BigDecimal("3")),
                        new Row("Bob", new BigDecimal("2"))
                )
        );
    }

    @Test
    public void test_groupAvgHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "3"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, AVG(distance) s FROM " + name + " GROUP BY name HAVING s > 2",
                asList(
                        new Row("Bob", new BigDecimal("2.5")),
                        new Row("Joey", new BigDecimal("3"))
                )
        );
    }

    @Test
    public void test_groupExpressionAvgHaving() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "3"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT name, AVG(distance * 2) s FROM " + name + " GROUP BY name HAVING s > 4",
                asList(
                        new Row("Bob", new BigDecimal("5")),
                        new Row("Joey", new BigDecimal("6"))
                )
        );
    }

    @SuppressWarnings("unused")
    private Object[] avg_types() {
        return new Object[]{
                new Object[]{QueryDataTypeFamily.TINYINT, QueryDataType.DECIMAL},
                new Object[]{QueryDataTypeFamily.SMALLINT, QueryDataType.DECIMAL},
                new Object[]{QueryDataTypeFamily.INTEGER, QueryDataType.DECIMAL},
                new Object[]{QueryDataTypeFamily.BIGINT, QueryDataType.DECIMAL},
                new Object[]{QueryDataTypeFamily.DECIMAL, QueryDataType.DECIMAL},
                new Object[]{QueryDataTypeFamily.REAL, QueryDataType.DOUBLE},
                new Object[]{QueryDataTypeFamily.DOUBLE, QueryDataType.DOUBLE},
        };
    }

    @Test
    @Parameters(method = "avg_types")
    public void test_orderByAvg(QueryDataTypeFamily columnType, QueryDataType expectedResultType) {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(VARCHAR, columnType),
                asList(
                        new String[]{"A", "1"},
                        new String[]{"B", "2"},
                        new String[]{"A", "3"},
                        new String[]{"B", "4"}
                )
        );

        assertRowsOrdered(
                "SELECT name, AVG(distance) avg_distance FROM " + name + " GROUP BY name ORDER BY avg_distance DESC",
                asList(
                        new Row("B", expectedResultType.convert(3)),
                        new Row("A", expectedResultType.convert(2))
                )
        );
    }

    @Test
    public void test_multipleAggregateFunctions() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT COUNT(*), MIN(distance), MAX(distance), SUM(distance), AVG(distance) FROM " + name,
                singletonList(new Row(3L, 1, 3, 6L, new BigDecimal("2")))
        );
    }

    @Test
    public void test_expressionFromAggregates() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT COUNT(*) * SUM(distance) FROM " + name,
                singletonList(new Row(18L))
        );
    }

    @Test
    public void test_nestedAggregation() {
        String name = createTable(
                new String[]{"Alice", "1"},
                new String[]{"Bob", "2"},
                new String[]{"Alice", "2"},
                new String[]{"Bob", "3"},
                new String[]{"Joey", "3"}
        );

        assertRowsAnyOrder(
                "SELECT MAX(avg_dist) " +
                        "FROM (SELECT name, AVG(distance) avg_dist FROM " + name + " GROUP BY name)",
                singletonList(
                        new Row(new BigDecimal("3"))
                )
        );
    }

    @Test
    public void test_aggregatingNonOrderedStreamingSource() {
        String name = randomName();
        TestStreamSqlConnector.create(sqlService, name, singletonList("v"), singletonList(QueryDataTypeFamily.BIGINT));

        assertThatThrownBy(() -> sqlService.execute("SELECT COUNT(*) FROM " + name))
                .hasRootCauseMessage("Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                        "(see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    @Test
    public void test_aggregatingNonOrderedStreamingFunction() {
        assertThatThrownBy(() -> sqlService.execute("SELECT COUNT(*) FROM TABLE(GENERATE_STREAM(1))"))
                .hasRootCauseMessage("Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                        "(see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    @Test
    public void test_distinctNonOrderedStreamingSource() {
        String name = randomName();
        TestStreamSqlConnector.create(sqlService, name, singletonList("v"), singletonList(QueryDataTypeFamily.BIGINT));

        assertThatThrownBy(() -> sqlService.execute("SELECT DISTINCT v FROM " + name))
                .hasRootCauseMessage("Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                        "(see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    @Test
    public void test_distinctNonOrderedStreamingFunction() {
        assertThatThrownBy(() -> sqlService.execute("SELECT DISTINCT v FROM TABLE(GENERATE_STREAM(1))"))
                .hasRootCauseMessage("Streaming aggregation is supported only for window aggregation, with imposed watermark order " +
                        "(see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    @Test
    public void test_rollup() {
        String name = createTable();
        assertThatThrownBy(
                () -> sqlService.execute("SELECT COUNT(*) FROM " + name + " GROUP BY ROLLUP(name, distance)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("ROLLUP not supported");
    }

    @Test
    public void test_cube() {
        String name = createTable();
        assertThatThrownBy(
                () -> sqlService.execute("SELECT COUNT(*) FROM " + name + " GROUP BY CUBE(name, distance)"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("CUBE not supported");
    }

    @Test
    public void test_groupingSet() {
        String name = createTable();
        assertThatThrownBy(
                () -> sqlService.execute("SELECT COUNT(*) FROM " + name + " GROUP BY GROUPING SETS ((name), (distance))"))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("GROUPING SETS not supported");
    }

    @Test
    public void test_streamAggregationWithoutWindowFunction() {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name", "distance"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR, INTEGER),
                new Object[]{timestampTz(0), "Alice", 1});

        assertThatThrownBy(() ->
                sqlService.execute("SELECT COUNT(name) " +
                        "FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)) " +
                        "GROUP BY ts"))
                .hasRootCauseMessage("Streaming aggregation is supported only for window aggregation, with imposed watermark order" +
                        " (see TUMBLE/HOP and IMPOSE_ORDER functions)");
    }

    private static String createTable(String[]... values) {
        String name = randomName();
        TestBatchSqlConnector.create(
                sqlService,
                name,
                asList("name", "distance"),
                asList(VARCHAR, INTEGER),
                asList(values)
        );
        return name;
    }
}
