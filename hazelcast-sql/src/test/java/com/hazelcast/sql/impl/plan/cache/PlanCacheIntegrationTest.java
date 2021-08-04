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

package com.hazelcast.sql.impl.plan.cache;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.exec.FaultyExec;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class PlanCacheIntegrationTest extends PlanCacheTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testPlanIsCached() {
        HazelcastInstance member = factory.newHazelcastInstance();
        IMap<Integer, Integer> map = member.getMap("map");
        map.put(1, 1);

        PlanCache planCache = getPlanCache(member);

        Plan plan = getPlan(member, "SELECT * FROM map");
        assertEquals(1, planCache.size());
        assertSame(plan, planCache.get(plan.getPlanKey()));

        Plan plan2 = getPlan(member, "SELECT * FROM map");
        assertEquals(1, planCache.size());
        assertSame(plan, planCache.get(plan.getPlanKey()));
        assertSame(plan, plan2);
    }

    @Test
    public void testPlanInvalidatedOnIndexAdd() {
        HazelcastInstance member = factory.newHazelcastInstance();
        IMap<Integer, Integer> map = member.getMap("map");
        map.put(1, 1);

        PlanCache planCache = getPlanCache(member);

        Plan plan = getPlan(member, "SELECT * FROM map");
        assertEquals(1, planCache.size());
        assertSame(plan, planCache.get(plan.getPlanKey()));

        map.addIndex(IndexType.SORTED, "this");

        assertTrueEventually(() -> {
            Plan plan2 = getPlan(member, "SELECT * FROM map");
            assertEquals(1, planCache.size());
            assertSame(plan2, planCache.get(plan2.getPlanKey()));
            assertNotSame(plan, plan2);
        });
    }

    @Test
    public void testPlanInvalidatedOnMapDestroy() {
        HazelcastInstance member = factory.newHazelcastInstance();
        IMap<Integer, Integer> map = member.getMap("map");
        map.put(1, 1);

        PlanCache planCache = getPlanCache(member);

        Plan plan = getPlan(member, "SELECT * FROM map");
        assertEquals(1, planCache.size());
        assertSame(plan, planCache.get(plan.getPlanKey()));

        map.destroy();
        assertTrueEventually(() -> assertEquals(0, planCache.size()));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Test
    public void testPlanInvalidatedOnMapSchemaChange() {
        HazelcastInstance member = factory.newHazelcastInstance();
        IMap map = member.getMap("map");
        map.put(1, 1);

        PlanCache planCache = getPlanCache(member);

        Plan plan = getPlan(member, "SELECT * FROM map");
        assertEquals(1, planCache.size());
        assertSame(plan, planCache.get(plan.getPlanKey()));

        map.clear();
        map.put("1", "1");

        assertTrueEventually(() -> {
            Plan plan2 = getPlan(member, "SELECT * FROM map");
            assertEquals(1, planCache.size());
            assertSame(plan2, planCache.get(plan2.getPlanKey()));
            assertNotSame(plan, plan2);
        });
    }

    @Test
    public void testPlanInvalidatedOnPartitionMigration() {
        HazelcastInstance member = factory.newHazelcastInstance();
        IMap<Integer, Integer> map = member.getMap("map");
        map.put(1, 1);

        PlanCache planCache = getPlanCache(member);

        Plan plan = getPlan(member, "SELECT * FROM map");
        assertEquals(1, planCache.size());
        assertSame(plan, planCache.get(plan.getPlanKey()));

        factory.newHazelcastInstance();

        assertTrueEventually(() -> {
            Plan plan2 = getPlan(member, "SELECT * FROM map");
            assertEquals(1, planCache.size());
            assertSame(plan2, planCache.get(plan2.getPlanKey()));
            assertNotSame(plan, plan2);
        });
    }

    @Test
    public void testPlanInvalidationOnExceptionWithInvalidationFlag() {
        HazelcastInstance member = factory.newHazelcastInstance();
        IMap<Integer, Integer> map = member.getMap("map");
        map.put(1, 1);

        PlanCache planCache = getPlanCache(member);

        Plan plan = getPlan(member, "SELECT * FROM map");
        assertEquals(1, planCache.size());
        assertSame(plan, planCache.get(plan.getPlanKey()));

        // Error without invalidation
        setExecHook(member, exec -> {
            if (exec instanceof MapScanExec) {
                exec = new FaultyExec(exec, QueryException.error("Failed"));
            }

            return exec;
        });
        executeWithException(member, "SELECT * FROM map");
        assertEquals(1, planCache.size());
        assertSame(plan, planCache.get(plan.getPlanKey()));

        // Error with invalidation
        setExecHook(member, exec -> {
            if (exec instanceof MapScanExec) {
                exec = new FaultyExec(exec, QueryException.error("Failed").markInvalidate());
            }

            return exec;
        });
        executeWithException(member, "SELECT * FROM map");
        assertEquals(0, planCache.size());
        assertNull(planCache.get(plan.getPlanKey()));
    }

    private PlanCache getPlanCache(HazelcastInstance instance) {
        return nodeEngine(instance).getSqlService().getPlanCache();
    }

    private Plan getPlan(HazelcastInstance instance, String sql) {
        try (SqlResult result = instance.getSql().execute(sql)) {
            SqlResultImpl result0 = (SqlResultImpl) result;

            return result0.getPlan();
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void executeWithException(HazelcastInstance instance, String sql) {
        try (SqlResult result = instance.getSql().execute(sql)) {
            for (SqlRow ignore : result) {
                // No-op.
            }

            fail("Must fail");
        } catch (Exception e) {
            // No-op.
        }
    }
}
