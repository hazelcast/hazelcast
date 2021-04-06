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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

import static com.hazelcast.sql.impl.SqlErrorCode.PARSING;
import static org.junit.Assert.assertEquals;

@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InOperatorIntegrationTest extends ExpressionTestSupport {
    protected String longList = "(0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 28, 31, 35)";
    private static final String inOperatorIncompatibleTypesError = "Values passed to IN operator must have compatible types";
    private static final String incompatibleTypesError = "Values in expression list must have compatible types";

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

    @Test
    public void inPredicateWithRawNullTest() {
        putAll(0, 1, 2);
        checkValues(sqlQuery("IN (NULL, 0, 1)"), SqlColumnType.INTEGER, new Integer[]{0, 1});
        checkValues(sqlQuery("IN (0, NULL, NULL, NULL, 2)"), SqlColumnType.INTEGER, new Integer[]{2, 0});
        checkValues(sqlQuery("NOT IN (0, NULL, NULL, NULL, 2)"), SqlColumnType.INTEGER, new Integer[]{1});
        checkValues(sqlQuery("NOT IN (NULL, 2, 1)"), SqlColumnType.INTEGER, new Integer[]{0});

        checkValues("SELECT this FROM map WHERE NOT (this IN (2, 1, 0))", SqlColumnType.INTEGER, new Integer[]{});
        checkValues("SELECT this FROM map WHERE NOT (this IN (2, NULL, 1))", SqlColumnType.INTEGER, new Integer[]{0});
        checkValues("SELECT this FROM map WHERE NOT (this IN (NULL, 2, 1, 0))", SqlColumnType.INTEGER, new Integer[]{});
        checkValues("SELECT this FROM map WHERE true <> (this IN (NULL, 2, 1, 0))", SqlColumnType.INTEGER, new Integer[]{});
    }

    @Test
    public void inPredicateWithSubQueryTest() {
        String expectedExMessage = "Sub-queries are not allowed for IN operator.";

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

        checkFailure0(sqlQuery("IN ('abc', 0, 1, 'bac')"),  PARSING, castError("VARCHAR", "INTEGER"));
        checkFailure0(sqlQuery("IN ('abc', TRUE)"),  PARSING, castError("VARCHAR", "INTEGER"));
        checkFailure0(sqlQuery("IN (CAST ('1970-01-01' AS DATE), 2, 'dac')"),  PARSING, inOperatorIncompatibleTypesError);
        checkFailure0(sqlQuery("IN (CAST ('00:00:00' AS TIME), 1, CAST ('00:00:02' AS TIME))"), PARSING, inOperatorIncompatibleTypesError);
    }

    @Test
    public void inPredicateStringTypeTest() {
        putAll("abc", "bac", "cba", "20");
        checkValues(sqlQuery("IN ('abc', 'cba')"), SqlColumnType.VARCHAR, new String[]{"cba", "abc"});
        checkValues(sqlQuery("IN ('abc', 'cba', 'bac')"), SqlColumnType.VARCHAR, new String[]{"cba", "abc", "bac"});
        checkValues(sqlQuery("NOT IN ('abc', '1', 'cba')"), SqlColumnType.VARCHAR, new String[]{"bac", "20"});

        checkFailure0(sqlQuery("IN (CAST('00:00:00' AS TIME), '1')"), PARSING, castError("TIME", "VARCHAR"));
        checkFailure0(sqlQuery("IN ('abc', 0, 1, 'bac')"), PARSING, castError("TINYINT", "VARCHAR"));
        checkFailure0(sqlQuery("IN (3, CAST('1970-01-01' AS DATE), 'dac')"), PARSING, castError("TINYINT", "DATE"));
        checkFailure0(sqlQuery("IN (CAST('00:00:00' AS TIME), 'dac')"), PARSING, castError("TIME", "VARCHAR"));
        checkFailure0(sqlQuery("IN (CAST('00:00:00' AS TIME), 1, CAST ('00:00:02' AS TIME))"), PARSING, castError("TINYINT", "TIME"));
    }

    @Test
    public void inPredicateDatesTest() {
        LocalDate date1 = LocalDate.of(1970, 1, 1);
        LocalDate date2 = LocalDate.of(1970, 1, 3);
        putAll(date1, date2);
        checkValues(sqlQuery("IN (CAST ('1970-01-01' AS DATE), CAST ('1970-01-03' AS DATE))"), SqlColumnType.DATE, new LocalDate[]{date1, date2});
        checkValues(sqlQuery("IN (CAST ('1970-01-01' AS DATE), CAST ('1970-01-02' AS DATE))"), SqlColumnType.DATE, new LocalDate[]{date1});
        checkValues(sqlQuery("NOT IN (CAST ('1970-01-01' AS DATE), CAST ('1970-01-02' AS DATE))"), SqlColumnType.DATE, new LocalDate[]{date2});

        checkFailure0(sqlQuery("IN ('abc', 0, 1, 'bac')"),  PARSING, castError("VARCHAR", "DATE"));
        checkFailure0(sqlQuery("IN (CAST('1970-01-01' AS DATE), 2)"),  PARSING, castError("TINYINT", "DATE"));
        checkFailure0(sqlQuery("IN (TRUE, CAST('00:00:00' AS TIME))"),  PARSING, incompatibleTypesError);
        checkFailure0(sqlQuery("IN (CAST ('00:00:00' AS TIME), 1, CAST ('00:00:02' AS TIME))"), PARSING, inOperatorIncompatibleTypesError);
    }

    @Test
    public void inPredicateTimesTest() {
        LocalTime time1 = LocalTime.of(0, 0, 0);
        LocalTime time2 = LocalTime.of(0, 0, 2);
        putAll(time1, time2);
        checkValues(sqlQuery("IN (CAST('00:00:00' AS TIME), CAST('00:00:02' AS TIME))"), SqlColumnType.TIME, new LocalTime[]{time1, time2});
        checkValues(sqlQuery("IN (CAST('00:00:00' AS TIME), CAST('00:00:01' AS TIME))"), SqlColumnType.TIME, new LocalTime[]{time1});
        checkValues(sqlQuery("NOT IN (CAST('00:00:00' AS TIME))"), SqlColumnType.TIME, new LocalTime[]{time2});

        checkFailure0(sqlQuery("IN ('abc', 0, 1, 'bac')"),  PARSING, castError("VARCHAR", "TIME"));
        checkFailure0(sqlQuery("IN (CAST('00:00:00' AS TIME), 1)"), PARSING, castError("TINYINT", "TIME"));
        checkFailure0(sqlQuery("IN (CAST('1970-01-01' AS DATE), 2)"),  PARSING, inOperatorIncompatibleTypesError);
        checkFailure0(sqlQuery("NOT IN (0, 1)"),  PARSING, inOperatorIncompatibleTypesError);
        checkFailure0(sqlQuery("IN (CAST ('1970-01-01' AS DATE), 1, CAST ('00:00:02' AS TIME))"), PARSING, inOperatorIncompatibleTypesError);
    }

    @Ignore(value = "Should be enabled after engines merge.")
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
        List<SqlRow> rows = execute(member, sql, params);
        assertEquals(expectedResults.length, rows.size());
        for (int i = 0; i < expectedResults.length; i++) {
            SqlRow row = rows.get(i);
            assertEquals(expectedType, row.getMetadata().getColumn(0).getType());
            assertEquals(expectedResults[i], row.getObject(0));
        }
    }

    private String sqlQuery(String inClause) {
        return "SELECT this FROM map WHERE this " + inClause;
    }

    private String castError(String fromType, String toType) {
        return fromType + " to " + toType;
    }

    private String sqlRowQuery(String inClause) {
        return "SELECT this FROM map WHERE (key, __this) " + inClause;
    }
}
