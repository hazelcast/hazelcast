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

package com.hazelcast.sql.impl.calcite.parse;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.JetSqlCoreBackend;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.calcite.HazelcastSqlBackend;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.SqlBackend;
import com.hazelcast.sql.impl.calcite.TestMapTable;
import com.hazelcast.sql.impl.calcite.TestTableResolver;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for unsupported operations in parser.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ParserOperationsTest extends SqlTestSupport {
    private static OptimizerContext context;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, smallInstanceConfig());
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance());
        JetSqlCoreBackend jetSqlService = nodeEngine.getService(JetSqlCoreBackend.SERVICE_NAME);
        context = createContext(new HazelcastSqlBackend(nodeEngine), (SqlBackend) jetSqlService.sqlBackend());
    }

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
    public void testOrderBy() {
        checkSuccess("SELECT a, b FROM t ORDER BY a");
        checkSuccess("SELECT a, b FROM t ORDER BY a ASC");
        checkSuccess("SELECT a, b FROM t ORDER BY a DESC");
        checkSuccess("SELECT a, b FROM t ORDER BY a DESC, b ASC");
        checkSuccess("SELECT a, b FROM t ORDER BY a DESC OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY");
        checkSuccess("SELECT a, b FROM t ORDER BY a DESC FETCH FIRST 20 ROWS ONLY");
        checkSuccess("SELECT a, b FROM t ORDER BY a DESC OFFSET 10 ROWS");
    }

    @Test
    public void testOffsetFetchOnly() {
        checkSuccess("SELECT a, b FROM t OFFSET 10 ROWS FETCH FIRST 20 ROWS ONLY");
        checkSuccess("SELECT a, b FROM t FETCH FIRST 20 ROWS ONLY");
        checkSuccess("SELECT a, b FROM t OFFSET 10 ROWS");
    }

    @Test
    public void testSelectScalar() {
        checkFailure("SELECT (SELECT a FROM t) FROM t", "SCALAR QUERY not supported");
    }

    @Test
    public void testWhereScalar() {
        checkFailure("SELECT a, b FROM t WHERE (SELECT a FROM t) IS NULL", "SCALAR QUERY not supported");
    }

    @Test
    public void testNullsFirstLast() {
        checkFailure("SELECT a, b FROM t ORDER BY a DESC NULLS FIRST", "Function 'NULLS FIRST' does not exist");
        checkFailure("SELECT a, b FROM t ORDER BY a DESC NULLS LAST", "Function 'NULLS LAST' does not exist");
    }

    @Test
    public void testUnsupportedGroupBy() {
        checkSuccess("SELECT a FROM t GROUP BY a");
    }

    @Test
    public void testUnsupportedAggregate() {
        checkSuccess("SELECT SUM(a) FROM t");
    }

    @Test
    public void testUnsupportedJoin() {
        checkSuccess("SELECT t1.a, t2.a FROM t t1 JOIN t t2 ON t1.a = t2.a");
    }

    @Test
    public void testMalformedExpression() {
        checkFailure("select 1 + from t", "Was expecting one of");
    }

    private static void checkSuccess(String sql) {
        context.parse(sql);
    }

    private static void checkFailure(String sql, String message) {
        try {
            context.parse(sql);

            fail("Exception is not thrown: " + message);
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.PARSING, e.getCode());

            assertTrue(e.getCause().getMessage(), e.getCause().getMessage().contains(message));
        }
    }

    private static OptimizerContext createContext(SqlBackend hzBackend, SqlBackend jetBackend) {
        PartitionedMapTable partitionedMapTable = new PartitionedMapTable(
                "public",
                "t",
                "t",
                Arrays.asList(TestMapTable.field("a"), TestMapTable.field("b")),
                new ConstantTableStatistics(100L),
                null,
                null,
                null,
                null,
                null,
                false
        );

        TableResolver resolver = TestTableResolver.create(
                "public",
                partitionedMapTable
        );
        List<TableResolver> tableResolvers = Collections.singletonList(resolver);
        List<List<String>> searchPaths = QueryUtils.prepareSearchPaths(emptyList(), tableResolvers);

        return OptimizerContext.create(
                new SqlCatalog(tableResolvers),
                searchPaths,
                emptyList(),
                1,
                hzBackend,
                jetBackend
        );
    }
}
