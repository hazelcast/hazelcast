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

package com.hazelcast.jet.sql.impl.misc;

import com.hazelcast.config.IndexType;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.opt.OptimizerTestSupport;
import com.hazelcast.jet.sql.impl.opt.logical.FullScanLogicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.map.IMap;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test that ensures that a security callback is invoked as expected.
 */
@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
public class SqlSecurityCallbackTest extends OptimizerTestSupport {
    private static final int mapSize = 1000;

    @Parameterized.Parameter
    public boolean useIndex;

    private String mapName;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, smallInstanceConfig());
    }

    @Parameterized.Parameters(name = "useIndex:{0}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();
        res.add(new Boolean[]{false});
        res.add(new Boolean[]{true});
        return res;
    }

    @Before
    public void before() {
        mapName = SqlTestSupport.randomName();
        createMapping(mapName, int.class, int.class);
        IMap<Integer, Integer> map = instance().getMap(mapName);
        if (useIndex) {
            map.addIndex(IndexType.HASH, "this");
        }
        populate(mapName, mapSize);
    }

    @Test
    public void test() {
        check("SELECT * FROM " + mapName + " WHERE this = 500", useIndex);
    }

    private void check(String sql, boolean useIndex) {
        // Execute twice to make sure that permission is checked when the plan is cached.
        for (int i = 0; i < 2; i++) {
            TestSqlSecurityContext securityContext = new TestSqlSecurityContext();

            try (SqlResult ignored = ((SqlServiceImpl) instance().getSql()).execute(new SqlStatement(sql), securityContext)) {
                // Check whether the index is used as expected.
                checkIndexUsage(sql, useIndex);

                // Check permissions.
                assertThat(securityContext.getPermissions()).contains(new MapPermission(mapName, ActionConstants.ACTION_READ));
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
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(instance().getMap(mapName)), mapTableFields),
                mapSize
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

    protected void populate(String mapName, int size) {
        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        instance().getMap(mapName).putAll(map);
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
