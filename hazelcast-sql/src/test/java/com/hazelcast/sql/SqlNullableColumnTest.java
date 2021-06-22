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
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.cache.PlanCacheTestSupport;
import org.junit.After;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class SqlNullableColumnTest extends PlanCacheTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testSelectWithNonNullSupport() {
        HazelcastInstance member = factory.newHazelcastInstance();
        IMap<String, Integer> map = member.getMap("map");
        map.put("key", 1);

        Plan plan = getPlan(member, "SELECT __key, 1 FROM map");
        List<SqlColumnMetadata> columns = plan.getRowMetadata().getColumns();
        assertEquals(columns.size(), 2);
        assertTrue(columns.get(0).isNullable());
        assertFalse(columns.get(1).isNullable());
    }

    private Plan getPlan(HazelcastInstance instance, String sql) {
        try (SqlResult result = instance.getSql().execute(sql)) {
            SqlResultImpl result0 = (SqlResultImpl) result;

            return result0.getPlan();
        }
    }
}
