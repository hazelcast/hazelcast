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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.Parameterized;

import java.util.List;

import static org.junit.Assert.assertEquals;

@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InPredicateIntegrationTest extends ExpressionTestSupport {

    @Test
    public void simpleInPredicateWithShortListTest() {
        putAll(0, 1);
        String sql = "SELECT * FROM map WHERE this IN (0, 1, 2)";
        List<SqlRow> rows = execute(member, sql);
        assertEquals(2, rows.size());
        assertEquals(0, (int) rows.get(0).getObject(0));
        assertEquals(1, (int) rows.get(1).getObject(0));
    }

    @Test
    public void simpleInPredicateWithShortListAndNullsWithinTest() {
        putAll(0, 1);
        String sql = "SELECT * FROM map WHERE this IN (NULL, 0, 1)";
        List<SqlRow> rows = execute(member, sql);
        assertEquals(2, rows.size());
        assertEquals(0, (int) rows.get(0).getObject(0));
        assertEquals(1, (int) rows.get(1).getObject(0));
    }

    @Test
    public void simpleInPredicateWithLongListTest() {
        putAll(0, 1, 20, 30);
        String sql = "SELECT * FROM map WHERE this IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22)";
        List<SqlRow> rows = execute(member, sql);
        assertEquals(3, rows.size());
        assertEquals(0, (int) rows.get(0).getObject(0));
        assertEquals(1, (int) rows.get(1).getObject(0));
        assertEquals(20, (int) rows.get(2).getObject(0));
    }
    
    @Test
    public void simpleInPredicateWithSubQueryTest() {
        putAll(1, 2);
        String sql = "SELECT * FROM map WHERE this IN (SELECT __key FROM map)";
        Assert.assertThrows(HazelcastSqlException.class, () -> execute(member, sql));
    }

    @Test
    public void simpleNotInPredicateWithListTest() {
        putAll(0, 1, 3);
        String sql = "SELECT * FROM map WHERE this NOT IN (0, 1, 2)";
        List<SqlRow> rows = execute(member, sql);
        assertEquals(1, rows.size());
        assertEquals(3, (int) rows.get(0).getObject(0));
    }

    @Test
    public void simpleNotInPredicateWithSubQueryTest() {
        putAll(1, 2);
        String sql = "SELECT * FROM map WHERE this NOT IN (SELECT __key FROM map)";
        Assert.assertThrows(HazelcastSqlException.class, () -> execute(member, sql));
    }
}
