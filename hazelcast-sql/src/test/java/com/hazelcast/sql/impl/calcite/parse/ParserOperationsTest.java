/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.parse;

import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.TestMapTable;
import com.hazelcast.sql.impl.calcite.TestTableResolver;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for unsupported operations in parser.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ParserOperationsTest {
    @Test
    public void testSelectColumn() {
        checkSuccess("SELECT a, b FROM t");
    }

    @Test
    public void testSelectLiteral() {
        checkSuccess("SELECT 1 FROM t");
    }

    @Test
    public void testSelectAs() {
        checkSuccess("SELECT a a_alias FROM t t_alias");
    }

    @Test
    public void testSelectFromDerivedTable() {
        checkSuccess("SELECT a_alias FROM (SELECT a a_alias FROM t)");
    }

    @Test
    public void testWhereIsPredicates() {
        checkSuccess("SELECT a, b FROM t WHERE a IS NULL");
    }

    @Test
    public void testWhereComparison() {
        checkSuccess("SELECT a, b FROM t WHERE a = b");
        checkSuccess("SELECT a, b FROM t WHERE a != b");
        checkSuccess("SELECT a, b FROM t WHERE a <> b");
        checkSuccess("SELECT a, b FROM t WHERE a > b");
        checkSuccess("SELECT a, b FROM t WHERE a >= b");
        checkSuccess("SELECT a, b FROM t WHERE a < b");
        checkSuccess("SELECT a, b FROM t WHERE a <= b");
    }

    @Test
    public void testUnsupportedSelectScalar() {
        checkFailure(
            "SELECT (SELECT a FROM t) FROM t",
            "SCALAR QUERY is not supported"
        );
    }

    @Test
    public void testUnsupportedWhereScalar() {
        checkFailure(
            "SELECT a, b FROM t WHERE (SELECT a FROM t) IS NULL",
            "SCALAR QUERY is not supported"
        );
    }

    @Test
    public void testUnsupportedOrderBy() {
        checkFailure(
            "SELECT a FROM t ORDER BY a",
            "ORDER BY is not supported"
        );
    }

    @Test
    public void testUnsupportedGroupBy() {
        checkFailure(
            "SELECT a FROM t GROUP BY a",
            "GROUP BY is not supported"
        );
    }

    @Test
    public void testUnsupportedAggregate() {
        checkFailure(
            "SELECT SUM(a) FROM t",
            "SUM is not supported"
        );
    }

    @Test
    public void testUnsupportedJoin() {
        checkFailure(
            "SELECT t1.a, t2.a FROM t t1 JOIN t t2 ON t1.a = t2.a",
            "JOIN is not supported"
        );
    }

    private static void checkSuccess(String sql) {
        createContext().parse(sql);
    }

    private static void checkFailure(String sql, String message) {
        try {
            createContext().parse(sql);

            fail("Exception is not thrown: " + message);
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.PARSING, e.getCode());

            assertTrue(e.getCause().getMessage(), e.getCause().getMessage().endsWith(message));
        }
    }

    private static OptimizerContext createContext() {
        TableResolver resolver = TestTableResolver.create(
            "public",
            TestMapTable.create("public", "t", TestMapTable.field("a"), TestMapTable.field("b"))
        );

        return OptimizerContext.create(
            Collections.singletonList(resolver),
            null,
            1
        );
    }
}
