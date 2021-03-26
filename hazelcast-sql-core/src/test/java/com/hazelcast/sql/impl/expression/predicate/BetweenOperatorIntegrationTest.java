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
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BetweenOperatorIntegrationTest extends ExpressionTestSupport {
    @Test
    public void betweenPredicateNumberTest() {
        putAll(0, 1, 25, 30);
        checkValues(sqlQuery("BETWEEN 0 AND 25"), SqlColumnType.INTEGER, new Integer[]{0, 1, 25});
        checkValues(sqlQuery("NOT BETWEEN 25 AND 40"), SqlColumnType.INTEGER, new Integer[]{0, 1});
        checkValues(sqlQuery("BETWEEN ? AND ?"), SqlColumnType.INTEGER, new Integer[]{1, 25}, 1, 25);
    }

    protected void checkValues(
        String sql,
        SqlColumnType expectedType,
        Object[] expectedResults,
        Object ... params
    ) {
        List<SqlRow> rows = execute(member, sql, params);
        rows.sort(Comparator.comparingInt(a -> a.getObject(0)));
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

}
