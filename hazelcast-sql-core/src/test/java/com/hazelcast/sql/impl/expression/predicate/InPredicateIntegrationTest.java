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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.time.LocalDate;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InPredicateIntegrationTest extends ExpressionTestSupport {
    protected String longList = "(0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 28, 31, 35)";

    @Test
    public void inPredicateWithDifferentListLengthTest() {
        putAll(0, 1, 25, 30);
        checkValues(sqlQuery("IN (0, 1, 2)"), SqlColumnType.INTEGER, new Integer[]{0, 1});
        checkValues(sqlQuery("IN (?, ?, ?)"), SqlColumnType.INTEGER, new Integer[]{0, 1}, 0, 1, 2);
        checkValues(sqlQuery("IN " + longList), SqlColumnType.INTEGER, new Integer[]{25, 0, 1});
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
    }

    @Test
    public void inPredicateWithSubQueryTest() {
        putAll(1, 2);
        checkThrows(sqlQuery("IN (SELECT __key FROM map)"), HazelcastSqlException.class);
        checkThrows(sqlQuery("NOT IN (SELECT __key FROM map)"), HazelcastSqlException.class);
    }

    @Test
    public void inPredicateNumberTypeTest() {
        putAll(0, 1);
        checkValues(sqlQuery("IN (0, '1', 2)"), SqlColumnType.INTEGER, new Integer[]{0, 1});
        checkValues(sqlQuery("IN ('0', 1, '2')"), SqlColumnType.INTEGER, new Integer[]{0, 1});
        checkValues(sqlQuery("IN (CAST ('0' AS INTEGER), CAST ('1' AS INTEGER), 2)"), SqlColumnType.INTEGER, new Integer[]{0, 1});
        checkValues(sqlQuery("NOT IN (CAST ('0' AS INTEGER), CAST ('1' AS INTEGER), 2)"), SqlColumnType.INTEGER, new Integer[]{});
    }

    @Test
    public void inPredicateStringTypeTest() {
        putAll("abc", "bac", "cba");
        checkValues(sqlQuery("IN ('abc', 'cba')"), SqlColumnType.VARCHAR, new String[]{"cba", "abc"});
        checkValues(sqlQuery("IN ('abc', 'cba', 'bac')"), SqlColumnType.VARCHAR, new String[]{"cba", "abc", "bac"});
        checkValues(sqlQuery("NOT IN ('abc', '1', 'cba')"), SqlColumnType.VARCHAR, new String[]{"bac"});
    }

    @Test
    public void inPredicateDatesTest() {
        LocalDate date1 = LocalDate.of(1970, 1, 1);
        LocalDate date2 = LocalDate.of(1970, 1, 3);
        putAll(date1, date2);
        checkValues(sqlQuery("IN (CAST ('1970-01-01' AS DATE), CAST ('1970-01-03' AS DATE))"), SqlColumnType.DATE, new LocalDate[]{date1, date2});
        checkValues(sqlQuery("IN (CAST ('1970-01-01' AS DATE), CAST ('1970-01-02' AS DATE))"), SqlColumnType.DATE, new LocalDate[]{date1});
        checkValues(sqlQuery("NOT IN (CAST ('1970-01-01' AS DATE), CAST ('1970-01-02' AS DATE))"), SqlColumnType.DATE, new LocalDate[]{date2});
    }

    @Test
    public void inPredicateWithDifferentTypesWouldFailsTest() {
        putAll(0, 1, "abc", LocalDate.of(1970, 1, 1));
        checkThrows(sqlQuery("IN ('1970-01-01', 1, '2')"), HazelcastSqlException.class);
        checkThrows(sqlQuery("IN (0, 1, 'bac')"), HazelcastSqlException.class);
        checkThrows(sqlQuery(sqlQuery("IN (CAST ('1970-01-01' AS DATE), 1, CAST ('1970-01-02' AS DATE))")), HazelcastSqlException.class);
    }

    @Ignore(value = "Should be enabled after engines merge.")
    @Test
    public void notInPredicateRowWithShortListTest() {
        putAll(0, 1);
        String inClause = "NOT IN (ROW(0, 0), ROW(1, 1), ROW(2, 2))";
        // TODO: write check
    }

    protected void checkValues(
        String sql,
        SqlColumnType expectedType,
        Object[] expectedResults,
        Object ... params
    ) {
        List<SqlRow> rows = execute(member, sql, params);
        assertEquals(expectedResults.length, rows.size());
        for (int i = 0; i < expectedResults.length; i++) {
            SqlRow row = rows.get(i);
            assertEquals(expectedType, row.getMetadata().getColumn(1).getType());
            assertEquals(expectedResults[i], row.getObject(1));
        }
    }

    protected void checkThrows(String sql, Class<? extends HazelcastException> expectedClass) {
        Assert.assertThrows(expectedClass, () -> execute(member, sql));
    }

    private String sqlQuery(String inClause) {
        return "SELECT * FROM map WHERE this " + inClause;
    }

    private String sqlRowQuery(String inClause) {
        return "SELECT * FROM map WHERE (key, __this) " + inClause;
    }
}
