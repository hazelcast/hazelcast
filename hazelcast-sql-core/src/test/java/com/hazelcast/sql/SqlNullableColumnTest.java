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

package com.hazelcast.sql;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.cache.PlanCacheTestSupport;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class SqlNullableColumnTest extends PlanCacheTestSupport {

    private final SqlTestInstanceFactory factory = SqlTestInstanceFactory.create();

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
