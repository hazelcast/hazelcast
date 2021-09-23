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

package com.hazelcast.jet.sql.impl.expression.predicate;

import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

import static com.hazelcast.sql.impl.SqlErrorCode.DATA_EXCEPTION;
import static com.hazelcast.sql.impl.SqlErrorCode.PARSING;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InOperatorIntegrationTest extends ExpressionTestSupport {

    protected String longList = "(0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 28, 31, 35)";

    @Test
    public void inPredicateWithDifferentListLengthTest() {
        putAll(0, 1, 25, 30);
        checkValues(sqlQuery("IN (0, 1, 2)"), SqlColumnType.INTEGER, new Integer[]{0, 1});
        checkValues(sqlQuery("IN (?, ?, ?)"), SqlColumnType.INTEGER, new Integer[]{0, 1}, 0, 1, 2);
        checkValues(sqlQuery("IN " + longList), SqlColumnType.INTEGER, new Integer[]{25, 0, 1});
        checkValues("SELECT this FROM map WHERE NOT (this IN (2, 1))", SqlColumnType.INTEGER, new Integer[]{25, 0, 30});
    }

    @Test
    public void notInPredicateWithDifferentListLengthTest() {
        putAll(0, 1, 3);
        checkValues(sqlQuery("NOT IN (0, 1, 2)"), SqlColumnType.INTEGER, new Integer[]{3});
        checkValues(sqlQuery("NOT IN (?, ?, ?)"), SqlColumnType.INTEGER, new Integer[]{3}, 0, 1, 2);
        checkValues(sqlQuery("NOT IN " + longList), SqlColumnType.INTEGER, new Integer[]{3});
    }

    /**
     * @see <a href="https://github.com/hazelcast/hazelcast/issues/18592">Linked issue.</a>
     */
    @Test
    public void inPredicateWithRawNullTest() {
        putAll(0, 1, 2);
        checkFailure0(sqlQuery("IN (NULL, 0, 1)"), PARSING, "Raw nulls are not supported for IN operator.");
        checkFailure0(sqlQuery("IN (0, NULL, NULL, NULL, 2)"), PARSING, "Raw nulls are not supported for IN operator.");

        checkFailure0(sqlQuery("NOT IN (NULL, 1)"), PARSING, "Raw nulls are not supported for IN operator.");
        checkFailure0(sqlQuery("NOT IN (0, NULL, NULL, NULL, 2)"), PARSING, "Raw nulls are not supported for IN operator.");

        checkFailure0("SELECT this FROM map WHERE NOT (this IN (NULL, 2, 1, 0))", PARSING, "Raw nulls are not supported for IN operator.");
        checkFailure0("SELECT this FROM map WHERE true <> (this IN (NULL, 2, 1, 0))", PARSING, "Raw nulls are not supported for IN operator.");
    }

    @Test
    public void inPredicateWithSubQueryTest() {
        String expectedExMessage = "Sub-queries are not supported for IN operator.";

        putAll(1, 2);
        checkFailure0(sqlQuery("IN (SELECT __key FROM map)"), PARSING, expectedExMessage);
        checkFailure0(sqlQuery("NOT IN (SELECT __key FROM map)"), PARSING, expectedExMessage);
    }

    @Test
    public void inPredicateLongNumericTypeTest() {
        putAll(0L, 1L, Long.MAX_VALUE - 1);
        checkValues(sqlQuery("IN (0, 1, 2)"), SqlColumnType.BIGINT, new Long[]{0L, 1L});
        checkValues(sqlQuery("NOT IN (0, 1, 2)"), SqlColumnType.BIGINT, new Long[]{Long.MAX_VALUE - 1});
        checkValues(sqlQuery("IN (CAST ('0' AS INTEGER), CAST ('1' AS INTEGER), 2)"), SqlColumnType.BIGINT,
                new Long[]{0L, 1L});
        checkValues(sqlQuery("NOT IN (CAST ('0' AS INTEGER), CAST ('1' AS INTEGER), 2)"), SqlColumnType.BIGINT,
                new Long[]{Long.MAX_VALUE - 1});
    }

    @Test
    public void inPredicateNumericTypesTest() {
        putAll(0, 1, (int) Byte.MAX_VALUE, Short.MAX_VALUE - 1);
        checkValues(sqlQuery("IN (0, 1, 2)"), SqlColumnType.INTEGER, new Integer[]{0, 1});
        checkValues(sqlQuery("NOT IN (0, 1, 2)"), SqlColumnType.INTEGER,
                new Integer[]{(int) Byte.MAX_VALUE, Short.MAX_VALUE - 1});
        checkValues(sqlQuery("IN (CAST ('0' AS INTEGER), CAST ('1' AS TINYINT), 2)"), SqlColumnType.INTEGER,
                new Integer[]{0, 1});
        checkValues(sqlQuery("NOT IN (CAST ('0' AS INTEGER), CAST ('1' AS INTEGER), 2, (CAST (32766 AS SMALLINT)))"),
                SqlColumnType.INTEGER,
                new Integer[]{(int) Byte.MAX_VALUE});

        checkFailure0(sqlQuery("IN ('abc', 'bac')"), PARSING, "CAST function cannot convert literal 'abc' to type INTEGER: Cannot parse VARCHAR value to INTEGER");
        checkFailure0(sqlQuery("IN (FALSE, TRUE)"), PARSING, "Values passed to IN operator must have compatible types");
        checkFailure0(sqlQuery("IN (CAST ('1970-01-01' AS DATE))"), PARSING, "Values passed to IN operator must have compatible types");
        checkFailure0(sqlQuery("IN (CAST ('00:00:00' AS TIME))"), PARSING, "Values passed to IN operator must have compatible types");
    }

    @Test
    public void inPredicateStringTypeTest() {
        putAll("abc", "bac", "cba", "20");
        checkValues(sqlQuery("IN ('abc', 'cba')"), SqlColumnType.VARCHAR, new String[]{"cba", "abc"});
        checkValues(sqlQuery("IN ('abc', 'cba', 'bac')"), SqlColumnType.VARCHAR, new String[]{"cba", "abc", "bac"});
        checkValues(sqlQuery("NOT IN ('abc', '1', 'cba')"), SqlColumnType.VARCHAR, new String[]{"bac", "20"});

        // in the following tests we convert VARCHAR to another type, but it fails because the map contains improper strings
        checkFailure0(sqlQuery("IN (1)"), DATA_EXCEPTION,
                "Cannot parse VARCHAR value to TINYINT");
        checkFailure0(sqlQuery("IN (128)"), DATA_EXCEPTION,
                "Cannot parse VARCHAR value to SMALLINT");
        checkFailure0(sqlQuery("IN (CAST('00:00:00' AS TIME))"), DATA_EXCEPTION,
                "Cannot parse VARCHAR value to TIME");
        checkFailure0(sqlQuery("IN (CAST('2020-01-01' AS DATE))"), DATA_EXCEPTION,
                "Cannot parse VARCHAR value to DATE");
        checkFailure0(sqlQuery("IN (CAST('2020-01-01T14:01' AS TIMESTAMP))"), DATA_EXCEPTION,
                "Cannot parse VARCHAR value to TIMESTAMP");
        checkFailure0(sqlQuery("IN (CAST('2020-01-01T14:01+02:00' AS TIMESTAMP WITH TIME ZONE))"), DATA_EXCEPTION,
                "Cannot parse VARCHAR value to TIMESTAMP WITH TIME ZONE");

        // in the following tests the conversion will succeed
        putAll("1", "2", "3");
        checkValues(sqlQuery("IN (1, 3, 4)"), SqlColumnType.VARCHAR, new String[]{"3", "1"});

        putAll("1", "2", "3", "129");
        checkValues(sqlQuery("IN (0, 3, 129)"), SqlColumnType.VARCHAR, new String[]{"3", "129"});

        putAll("01:00", "01:01", "01:02");
        checkValues(sqlQuery("IN (CAST('01:01' AS TIME), CAST('01:04' AS TIME))"), SqlColumnType.VARCHAR, new String[]{"01:01"});

        putAll("2021-01-01", "2021-01-02", "2021-01-03");
        checkValues(sqlQuery("IN (CAST('2021-01-02' AS DATE), CAST('2021-01-03' AS DATE), CAST('2021-01-04' AS DATE))"),
                SqlColumnType.VARCHAR, new String[]{"2021-01-02", "2021-01-03"});

        putAll("2021-01-01T01:01", "2021-01-02T01:01");
        checkValues(sqlQuery("IN (CAST('2021-01-02T01:01' AS TIMESTAMP), CAST('2021-01-03T01:01' AS TIMESTAMP))"),
                SqlColumnType.VARCHAR, new String[]{"2021-01-02T01:01"});

        putAll("2021-01-01T01:01+01:00", "2021-01-02T01:01+01:00");
        checkValues(sqlQuery("IN (CAST('2021-01-02T01:01+01:00' AS TIMESTAMP WITH TIME ZONE), CAST('2021-01-03T01:01+01:00' AS TIMESTAMP WITH TIME ZONE))"),
                SqlColumnType.VARCHAR, new String[]{"2021-01-02T01:01+01:00"});
    }

    @Test
    public void inPredicateDatesTest() {
        LocalDate date1 = LocalDate.of(1970, 1, 1);
        LocalDate date2 = LocalDate.of(1970, 1, 3);
        putAll(date1, date2);
        checkValues(sqlQuery("IN (CAST ('1970-01-01' AS DATE), CAST ('1970-01-03' AS DATE))"), SqlColumnType.DATE, new LocalDate[]{date1, date2});
        checkValues(sqlQuery("IN (CAST ('1970-01-01' AS DATE), CAST ('1970-01-02' AS DATE))"), SqlColumnType.DATE, new LocalDate[]{date1});
        checkValues(sqlQuery("NOT IN (CAST ('1970-01-01' AS DATE), CAST ('1970-01-02' AS DATE))"), SqlColumnType.DATE, new LocalDate[]{date2});

        checkFailure0(sqlQuery("IN ('abc', 'bac')"), PARSING, "CAST function cannot convert literal 'abc' to type DATE: Cannot parse VARCHAR value to DATE");
        checkFailure0(sqlQuery("IN (2, 3)"), PARSING, "Values passed to IN operator must have compatible types");
        checkFailure0(sqlQuery("IN (TRUE, FALSE)"), PARSING, "Values passed to IN operator must have compatible types");
    }

    @Test
    public void inPredicateTimeTest() {
        LocalTime time1 = LocalTime.of(0, 0, 0);
        LocalTime time2 = LocalTime.of(0, 0, 2);
        putAll(time1, time2);
        checkValues(sqlQuery("IN (CAST('00:00:00' AS TIME), CAST('00:00:02' AS TIME))"), SqlColumnType.TIME, new LocalTime[]{time1, time2});
        checkValues(sqlQuery("IN (CAST('00:00:00' AS TIME), CAST('00:00:01' AS TIME))"), SqlColumnType.TIME, new LocalTime[]{time1});
        checkValues(sqlQuery("NOT IN (CAST('00:00:00' AS TIME))"), SqlColumnType.TIME, new LocalTime[]{time2});

        checkFailure0(sqlQuery("IN ('abc', 'bac')"), PARSING, "CAST function cannot convert literal 'abc' to type TIME: Cannot parse VARCHAR value to TIME");
        checkFailure0(sqlQuery("IN (1)"), PARSING, "Values passed to IN operator must have compatible types");
        checkFailure0(sqlQuery("IN (CAST('1970-01-01' AS DATE))"), PARSING, "Values passed to IN operator must have compatible types");
        checkFailure0(sqlQuery("NOT IN (0, 1)"), PARSING, "Values passed to IN operator must have compatible types");
        checkFailure0(sqlQuery("IN (CAST ('1970-01-01' AS DATE), 1, CAST ('00:00:02' AS TIME))"), PARSING, "Values passed to IN operator must have compatible types");
    }

    @Test
    public void test_mixedTypesInTheInList() {
        putAll(true);
        // we do not support values of mixed types
        checkFailure0(sqlQuery("IN (1, 'abc')"), PARSING, "Values in expression list must have compatible types");
        checkFailure0(sqlQuery("IN ('abc', 1)"), PARSING, "Values in expression list must have compatible types");
        checkFailure0(sqlQuery("IN ('abc', CAST('00:00:00' AS TIME))"), PARSING,
                "Values in expression list must have compatible types");

        // we do support values of mixed integers, mixed inexact numbers and temporal values
        putAll("1", "2");
        checkValues(sqlQuery("IN (1, 194919940239)"), SqlColumnType.VARCHAR, new String[]{"1"});
        checkValues(sqlQuery("IN (cast(1.0 as REAL), cast(2.3 as DOUBLE))"), SqlColumnType.VARCHAR, new String[]{"1"});
        putAll(
                LocalDateTime.of(2021, 1, 2, 0, 0, 0)
                        .atZone(ZoneId.systemDefault())
                        .toOffsetDateTime()
                        .toString(),
                "2021-01-02T01:03:04+01:00"
        );
        checkValues(sqlQuery("NOT IN (CAST('2021-01-02' AS DATE), CAST('2021-01-02T01:03:04+01:00' AS TIMESTAMP WITH TIME ZONE))"),
                SqlColumnType.VARCHAR, new String[0]);

        putAll("2021-01-02T00:00:00+12:00", "2021-01-02T01:03:04+01:00");
        checkValues(sqlQuery("NOT IN (CAST('2021-01-02' AS DATE), CAST('2021-01-02T01:03:04+01:00' AS TIMESTAMP WITH TIME ZONE))"),
                SqlColumnType.VARCHAR, new String[]{"2021-01-02T00:00:00+12:00"});

        // this is a bug in Calcite that when an integer is followed by temporal type,
        // it will try to coerce all right-hand values to temporal,
        // and an assert inside Calcite code will fail later. ¯\_(ツ)_/¯
        assertThatThrownBy(() -> execute(sqlQuery("IN (3, CAST('1970-01-01' AS DATE))")))
                .isInstanceOf(AssertionError.class)
                .hasMessage(null);
    }

    @Ignore(value = "Support ROW().")
    @Test
    public void inPredicateRowsTest() {
        putAll(0, 1);
        String inClause = "IN (ROW(0, 0), ROW(1, 1), ROW(2, 2))";
        // TODO: write check
    }

    protected void checkValues(
            String sql,
            SqlColumnType expectedType,
            Object[] expectedResults,
            Object... params
    ) {
        List<SqlRow> rows = execute(sql, params);
        assertTrue(rows.stream().allMatch(row -> row.getMetadata().getColumn(0).getType().equals(expectedType)));
        List<Object> rowValues = rows.stream().map(row -> row.getObject(0)).collect(Collectors.toList());
        assertThat(rowValues).containsExactlyInAnyOrderElementsOf(asList(expectedResults));
    }

    private String sqlQuery(String inClause) {
        return "SELECT this FROM map WHERE this " + inClause;
    }
}
