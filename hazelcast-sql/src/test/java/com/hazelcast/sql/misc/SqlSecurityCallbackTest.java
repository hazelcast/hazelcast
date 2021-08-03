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

package com.hazelcast.sql.misc;

import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.map.IMap;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.sql.impl.SqlTestSupport.getMapContainer;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test that ensures that a security callback is invoked as expected.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlSecurityCallbackTest extends OptimizerTestSupport {

    private static final String MAP_NAME = "map";

    @BeforeClass
    public static void beforeClass() {
        initialize(1, smallInstanceConfig());
    }

    @Before
    public void before() {
        IMap<Integer, Integer> map = instance().getMap(MAP_NAME);
        map.addIndex(IndexType.HASH, "this");
        map.put(1, 1);
    }

    @Test
    public void testScan() {
        check("SELECT * FROM " + MAP_NAME, false);
    }

    @Test
    public void testIndexScan() {
        check("SELECT * FROM " + MAP_NAME + " WHERE this = 1", true);
    }

    private void check(String sql, boolean useIndex) {
        // Execute twice to make sure that permission is checked when the plan is cached.
        for (int i = 0; i < 2; i++) {
            TestSqlSecurityContext securityContext = new TestSqlSecurityContext();

            try (SqlResult ignored = ((SqlServiceImpl) instance().getSql()).execute(new SqlStatement(sql), securityContext)) {
                // Check whether the index is used as expected.
                checkIndexUsage(sql, useIndex);

                // Check permissions.
                assertThat(securityContext.getPermissions()).contains(new MapPermission(MAP_NAME, ActionConstants.ACTION_READ));
            }
        }
    }

    private void checkIndexUsage(String sql, boolean expectedIndexUsage) {
        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.INT);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("this", QueryDataType.INT, false, QueryPath.VALUE_PATH)
        );
        HazelcastTable table = partitionedTable(
                MAP_NAME,
                mapTableFields,
                getPartitionedMapIndexes(getMapContainer(instance().getMap(MAP_NAME)), mapTableFields),
                false,
                1
        );
        OptimizerTestSupport.Result optimizationResult = optimizePhysical(sql, parameterTypes, table);
        assertPlan(
                optimizationResult.getLogical(),
                plan(planRow(0, FullScanLogicalRel.class))
        );
        assertPlan(
                optimizationResult.getPhysical(),
                plan(planRow(0, expectedIndexUsage ? IndexScanMapPhysicalRel.class : FullScanPhysicalRel.class))
        );
    }

    private static class TestSqlSecurityContext implements SqlSecurityContext {

        private final List<Permission> permissions = new ArrayList<>();

        @Override
        public boolean isSecurityEnabled() {
            return true;
        }

        @Override
        public void checkPermission(Permission permission) {
            permissions.add(permission);
        }

        private List<Permission> getPermissions() {
            return permissions;
        }
    }
}
