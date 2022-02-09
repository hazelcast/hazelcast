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
import com.hazelcast.jet.sql.impl.connector.test.TestAllTypesSqlConnector;
import com.hazelcast.sql.SqlService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SqlAggregateTest_TypeCoercion extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        sqlService = instance().getSql();
    }

    @Before
    public void before() {
        TestAllTypesSqlConnector.create(sqlService, "allTypesTable");
    }

    @Test
    public void test_groupBy() {
        TestAllTypesSqlConnector.create(sqlService, "t");
        String allFields = TestAllTypesSqlConnector.FIELD_LIST.stream()
                .map(f -> "\"" + f.name() + '"')
                .collect(Collectors.joining(", "));
        assertRowsAnyOrder(
                "SELECT " + allFields + " " +
                        "FROM t " +
                        "GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15",
                singletonList(TestAllTypesSqlConnector.ALL_TYPES_ROW));
    }

    @Test
    public void test_count() {
        String allFields = TestAllTypesSqlConnector.FIELD_LIST.stream()
                .map(f -> "COUNT(\"" + f.name() + "\")")
                .collect(Collectors.joining(", "));

        assertRowsAnyOrder("select " +
                        allFields +
                        ", count(null) " +
                        "from allTypesTable",
                Collections.singleton(new Row(1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 0L, 0L)));
    }

    @Test
    public void test_sum_numberTypes() {
        assertRowsAnyOrder("select " +
                        "sum(byte), " +
                        "sum(short), " +
                        "sum(\"int\"), " +
                        "sum(long), " +
                        "sum(\"float\"), " +
                        "sum(\"double\"), " +
                        "sum(\"decimal\")," +
                        "sum(null) " +
                        "from allTypesTable",
                Collections.singleton(new Row(
                        127L,
                        32767L,
                        2147483647L,
                        new BigDecimal(9223372036854775807L),
                        1234567890.1f,
                        123451234567890.1,
                        new BigDecimal("9223372036854775.123"),
                        null)));
    }

    @Test
    public void test_sum_otherTypes() {
        assertThatThrownBy(() -> sqlService.execute("select sum(string) from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [VARCHAR]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"boolean\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [BOOLEAN]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"time\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [TIME]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"date\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [DATE]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"timestamp\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [TIMESTAMP]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"timestampTz\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [TIMESTAMP WITH TIME ZONE]");
        assertThatThrownBy(() -> sqlService.execute("select sum(\"object\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'SUM' function to [OBJECT]");
    }

    @Test
    public void test_avg_numericTypes() {
        assertRowsAnyOrder("select " +
                        "avg(byte), " +
                        "avg(short), " +
                        "avg(\"int\"), " +
                        "avg(long), " +
                        "avg(\"float\"), " +
                        "avg(\"double\"), " +
                        "avg(\"decimal\")," +
                        "avg(null) " +
                        "from allTypesTable",
                Collections.singleton(new Row(
                        BigDecimal.valueOf(127),
                        BigDecimal.valueOf(32767),
                        BigDecimal.valueOf(2147483647),
                        BigDecimal.valueOf(9223372036854775807L),
                        (double) 1234567890.1f,
                        123451234567890.1,
                        new BigDecimal("9223372036854775.123"),
                        null)));
    }

    @Test
    public void test_avg_otherTypes() {
        assertThatThrownBy(() -> sqlService.execute("select avg(string) from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [VARCHAR]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"boolean\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [BOOLEAN]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"time\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [TIME]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"date\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [DATE]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"timestamp\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [TIMESTAMP]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"timestampTz\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [TIMESTAMP WITH TIME ZONE]");
        assertThatThrownBy(() -> sqlService.execute("select avg(\"object\") from allTypesTable"))
                .hasMessageContaining("Cannot apply 'AVG' function to [OBJECT]");
    }

    @Test
    public void test_min() {
        TestAllTypesSqlConnector.create(sqlService, "t");
        String allFields = TestAllTypesSqlConnector.FIELD_LIST.stream()
                .map(f -> "MIN(\"" + f.name() + "\")")
                .collect(Collectors.joining(", "));

        assertRowsAnyOrder("SELECT " + allFields + "FROM t",
                singletonList(TestAllTypesSqlConnector.ALL_TYPES_ROW));

        assertRowsAnyOrder("select min(null) from allTypesTable", singletonList(new Row((Object) null)));
    }

    @Test
    public void test_max() {
        TestAllTypesSqlConnector.create(sqlService, "t");
        String allFields = TestAllTypesSqlConnector.FIELD_LIST.stream()
                .map(f -> "MAX(\"" + f.name() + "\")")
                .collect(Collectors.joining(", "));

        assertRowsAnyOrder("SELECT " + allFields + "FROM t",
                singletonList(TestAllTypesSqlConnector.ALL_TYPES_ROW));

        assertRowsAnyOrder("select max(null) from allTypesTable", singletonList(new Row((Object) null)));
    }
}
