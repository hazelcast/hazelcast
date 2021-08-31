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

package com.hazelcast.jet.sql.impl.cache;

import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.plan.cache.PlanCache;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PlanCacheTest extends PlanCacheTestSupport {

    @BeforeClass
    public static void setUp() {
        initialize(1, smallInstanceConfig());
    }

    @Test
    public void testBasicOperations() {
        PlanCache cache = new PlanCache(10);

        PlanKey key = createKey("sql");

        // Put plan
        SqlPlan plan1 = createPlan(key, PART_MAP_1);
        cache.put(key, plan1);
        assertEquals(1, cache.size());
        assertSame(plan1, cache.get(key));

        // Overwrite plan
        SqlPlan plan2 = createPlan(key, PART_MAP_2);
        cache.put(key, plan2);
        assertEquals(1, cache.size());
        assertSame(plan2, cache.get(key));

        // Invalidate the plan that is no longer cached
        cache.invalidate(plan1);
        assertEquals(1, cache.size());
        assertSame(plan2, cache.get(key));

        // Invalidate cache plan
        cache.invalidate(plan2);
        assertEquals(0, cache.size());
        assertNull(cache.get(key));

        // Clear
        cache.put(key, plan1);
        assertEquals(1, cache.size());
        assertSame(plan1, cache.get(key));
        cache.clear();
        assertEquals(0, cache.size());
        assertNull(cache.get(key));
    }

    @Test
    public void testOverflow() {
        // Fill till full
        int size = 5;

        PlanCache cache = new PlanCache(size);

        for (int i = 0; i < size; i++) {
            PlanKey key = createKey(Integer.toString(i));

            cache.put(key, createPlan(key, PART_MAP_1));

            advanceTime();
        }

        assertEquals(size, cache.size());

        // Overflow happens here
        PlanKey overflowKey = createKey(Integer.toString(size));
        cache.put(overflowKey, createPlan(overflowKey, PART_MAP_1));

        assertEquals(size, cache.size());

        for (int i = 0; i < size; i++) {
            PlanKey key = createKey(Integer.toString(i + 1));

            assertNotNull(cache.get(key));
        }
    }

    @Test
    public void testPlanUsageUpdate() {
        PlanCache cache = new PlanCache(10);
        PlanKey key = createKey("sql");
        SqlPlan plan = createPlan(key, PART_MAP_1);

        cache.put(key, plan);
        long timestamp1 = plan.getPlanLastUsed();
        assertTrue(timestamp1 > 0);

        advanceTime();
        cache.put(key, plan);
        long timestamp2 = plan.getPlanLastUsed();
        assertTrue(timestamp2 > timestamp1);

        advanceTime();
        cache.get(key);
        long timestamp3 = plan.getPlanLastUsed();
        assertTrue(timestamp3 > timestamp2);
    }
}
