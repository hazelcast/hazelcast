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

package com.hazelcast.sql.impl;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.DefaultNodeExtension;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.impl.calcite.SqlBackend;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.plan.cache.PlanObjectKey;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.mocknetwork.MockNodeContext;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;

import static com.hazelcast.instance.impl.HazelcastInstanceFactory.newHazelcastInstance;
import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JetSqlTest extends SqlTestSupport {

    private static final TestHazelcastInstanceFactory FACTORY = new TestHazelcastInstanceFactory(1);

    private static final String JET_NAMESPACE = "jet-namespace";
    private static final String JET_TABLE = "jet-table";

    private static final Table TEST_TABLE = new TestTable();

    @Mock
    private JetSqlService jetSqlService;

    @Mock
    private TableResolver tableResolver;

    @Mock
    private SqlBackend sqlBackend;

    @Mock
    private SqlResult sqlResult;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @AfterClass
    public static void afterClass() {
        System.clearProperty(SqlServiceImpl.OPTIMIZER_CLASS_PROPERTY_NAME);

        FACTORY.shutdownAll();
    }

    @Test
    public void testJetSqlExecution() {
        // given
        given(tableResolver.getDefaultSearchPaths()).willReturn(singletonList(asList(CATALOG, JET_NAMESPACE)));
        given(tableResolver.getTables()).willReturn(singletonList(TEST_TABLE));

        given(jetSqlService.tableResolvers()).willReturn(singletonList(tableResolver));
        given(jetSqlService.sqlBackend()).willReturn(sqlBackend);
        given(jetSqlService.execute(any(SqlPlan.class), anyList(), anyLong(), anyInt())).willReturn(sqlResult);

        System.setProperty(SqlServiceImpl.OPTIMIZER_CLASS_PROPERTY_NAME, TestSqlOptimizer.class.getName());
        HazelcastInstance member = newHazelcastInstance(new Config(), randomName(), nodeContext(jetSqlService));

        // when
        SqlResult result = member.getSql().query("SELECT * FROM t");

        // then
        assertEquals(sqlResult, result);
    }

    private static NodeContext nodeContext(JetSqlService jetSqlService) {
        return new MockNodeContext(FACTORY.getRegistry(), FACTORY.nextAddress()) {
            @Override
            public NodeExtension createNodeExtension(Node node) {
                return new DefaultNodeExtension(node) {
                    @Override
                    public Map<String, Object> createExtensionServices() {
                        return ImmutableMap.of(JetSqlService.SERVICE_NAME, jetSqlService);
                    }
                };
            }
        };
    }

    private static class TestSqlOptimizer implements SqlOptimizer {

        @SuppressWarnings("checkstyle:RedundantModifier")
        public TestSqlOptimizer(
                @SuppressWarnings("unused") NodeEngine nodeEngine,
                JetSqlService jetSqlService
        ) {
            assertNotNull(jetSqlService);
        }

        @Override
        public SqlPlan prepare(OptimizationTask task) {
            assertContains(task.getSearchPaths(), asList(CATALOG, JET_NAMESPACE));
            assertEquals(
                    task.getSchema().getSchemas(),
                    ImmutableMap.of(JET_NAMESPACE, ImmutableMap.of(JET_TABLE, TEST_TABLE))
            );

            return mock(SqlPlan.class);
        }
    }

    private static class TestTable extends Table {

        private TestTable() {
            super(JET_NAMESPACE, JET_TABLE, emptyList(), new ConstantTableStatistics(0));
        }

        @Override
        public PlanObjectKey getObjectKey() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            return o == this;
        }
    }
}
