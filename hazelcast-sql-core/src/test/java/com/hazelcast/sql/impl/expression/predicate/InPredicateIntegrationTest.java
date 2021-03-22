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

import java.util.List;

import static org.junit.Assert.assertEquals;

@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InPredicateIntegrationTest extends ExpressionTestSupport {
    protected String longList = "(0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 28, 31, 35)";

    @Test
    public void inPredicateWithShortListTest() {
        putAll(0, 1);
        String inClause = "IN (0, 1, 2)";
        checkValues(sqlQuery(inClause), SqlColumnType.INTEGER, new Integer[]{0, 1});
    }

    @Ignore(value = "Should be enabled after engines merge.")
    @Test
    public void inPredicateRowWithShortListTest() {
        putAll(0, 1);
        String inClause = "IN (ROW(0, 0), ROW(1, 1), ROW(2, 2))";
        checkValues(sqlQuery(inClause), SqlColumnType.INTEGER, new Integer[]{0, 1});
        // TODO: write check
    }

    @Test
    public void inPredicateWithShortListAndNullsWithinTest() {
        putAll(0, 1);
        String inClause = "IN (NULL, 0, 1)";
        checkValues(sqlQuery(inClause), SqlColumnType.INTEGER, new Integer[]{0, 1});
    }

    @Test
    public void inPredicateWithLongListTest() {
        putAll(0, 1, 25, 30);
        String inClause = "IN " + longList;
        checkValues(sqlQuery(inClause), SqlColumnType.INTEGER, new Integer[]{25, 0, 1});
    }

    @Test
    public void inPredicateWithSubQueryTest() {
        putAll(1, 2);
        String inClause = "IN (SELECT __key FROM map)";
        checkThrows(sqlQuery(inClause), HazelcastSqlException.class);
    }

    @Test
    public void notInPredicateWithShortListTest() {
        putAll(0, 1, 3);
        String inClause = "NOT IN (0, 1, 2)";
        checkValues(sqlQuery(inClause), SqlColumnType.INTEGER, new Integer[]{3});
    }

    @Ignore(value = "Should be enabled after engines merge.")
    @Test
    public void notInPredicateRowWithShortListTest() {
        putAll(0, 1);
        String inClause = "NOT IN (ROW(0, 0), ROW(1, 1), ROW(2, 2))";
        // TODO: write check
    }

    @Test
    public void notInPredicateWithLongListTest() {
        putAll(0, 1, 3);
        String inClause = "NOT IN " + longList;
        checkValues(sqlQuery(inClause), SqlColumnType.INTEGER, new Integer[]{3});
    }

    @Test
    public void notInPredicateWithSubQueryTest() {
        putAll(1, 2);
        String inClause = "NOT IN (SELECT __key FROM map)";
        checkThrows(sqlQuery(inClause), HazelcastSqlException.class);
    }

    protected void checkValues(
        String sql,
        SqlColumnType expectedType,
        Object[] expectedResults
    ) {
        List<SqlRow> rows = execute(member, sql);
        assertEquals(expectedResults.length, rows.size());
        for (int i = 0; i < expectedResults.length; i++) {
            SqlRow row = rows.get(i);
            assertEquals(expectedType, row.getMetadata().getColumn(0).getType());
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