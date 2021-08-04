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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test that ensures that a security callback is invoked as expected.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class SqlSecurityCallbackTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
    private HazelcastInstance member;

    @Before
    public void before() {
        member = factory.newHazelcastInstance(getConfig());

        IMap<Integer, Integer> map = member.getMap(MAP_NAME);
        map.addIndex(IndexType.SORTED, "this");
        map.put(1, 1);
    }

    @After
    public void after() {
        factory.shutdownAll();
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

            try (SqlResult result = ((SqlServiceImpl) member.getSql()).execute(new SqlStatement(sql), securityContext)) {
                // Check whether the index is used as expected.
                MapIndexScanPlanNode indexScanNode = findFirstIndexNode(result);

                if (useIndex) {
                    assertNotNull(indexScanNode);
                } else {
                    assertNull(indexScanNode);
                }

                // Check permissions.
                List<Permission> permissions = securityContext.getPermissions();
                assertEquals(1, permissions.size());

                Permission permission = permissions.get(0);
                assertTrue(permission instanceof MapPermission);

                MapPermission mapPermission = (MapPermission) permission;
                MapPermission expectedMapPermission = new MapPermission(MAP_NAME, ActionConstants.ACTION_READ);

                assertEquals(expectedMapPermission.getName(), mapPermission.getName());
                assertEquals(expectedMapPermission.getActions(), mapPermission.getActions());
            }
        }
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
