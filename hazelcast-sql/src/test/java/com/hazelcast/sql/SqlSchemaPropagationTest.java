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

package com.hazelcast.sql;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlSchemaPropagationTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final String SCHEMA_NAME = "schema";

    private final TestHazelcastFactory factory = new TestHazelcastFactory(2);

    private HazelcastInstance member;
    private HazelcastInstance client;

    @Before
    public void before() {
        member = factory.newHazelcastInstance();
        client = factory.newHazelcastClient();

        member.getMap(MAP_NAME).put(1, 1);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testMember() {
        check(member);
    }

    @Test
    public void testClient() {
        check(client);
    }

    private void check(HazelcastInstance target) {
        // Set the wrapped optimizer to track optimization requests.
        SqlServiceImpl service = (SqlServiceImpl) member.getSql();

        WrappedSqlOptimizer optimizer = new WrappedSqlOptimizer(service.getOptimizer());

        service.setOptimizer(optimizer);

        // Execute the query from the target without schema.
        SqlStatement statement = new SqlStatement("SELECT __key FROM map");

        List<SqlRow> rows = executeStatement(target, statement);
        assertEquals(1, rows.size());
        assertEquals(1, (int) rows.get(0).getObject(0));

        assertEquals(1, service.getPlanCache().size());

        List<List<String>> originalSearchPaths = optimizer.getSearchPaths();

        // Execute the query from the target with schema.
        statement.setSchema(SCHEMA_NAME);

        rows = executeStatement(target, statement);
        assertEquals(1, rows.size());
        assertEquals(1, (int) rows.get(0).getObject(0));

        assertEquals(2, service.getPlanCache().size());

        List<List<String>> searchPaths = optimizer.getSearchPaths();

        List<List<String>> expectedSearchPaths = new ArrayList<>(originalSearchPaths);
        expectedSearchPaths.add(0, asList(CATALOG, SCHEMA_NAME));

        assertEquals(expectedSearchPaths, searchPaths);
    }

    private static final class WrappedSqlOptimizer implements SqlOptimizer {

        private final SqlOptimizer delegate;
        private List<List<String>> searchPaths;

        private WrappedSqlOptimizer(SqlOptimizer delegate) {
            this.delegate = delegate;
        }

        @Override
        public SqlPlan prepare(OptimizationTask task) {
            searchPaths = task.getSearchPaths();

            return delegate.prepare(task);
        }

        private List<List<String>> getSearchPaths() {
            return searchPaths;
        }
    }
}
