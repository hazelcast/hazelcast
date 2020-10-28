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

package com.hazelcast.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.TimeUtil;
import com.hazelcast.map.IMap;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ManageableClockComplexTest extends HazelcastTestSupport {

    private static final int NODE_COUNT = 2;

    private TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(NODE_COUNT);

    @Test
    public void testInstancesWithDifferentClocks() {
        HazelcastInstance instance1 = instanceFactory.newHazelcastInstance();
        HazelcastInstance instance2 = instanceFactory.newHazelcastInstance();

        ManageableClock clockOfInstance1 = clockOf(instance1);
        ManageableClock clockOfInstance2 = clockOf(instance2);

        clockOfInstance1.manage().advanceMillis(1000);
        clockOfInstance2.manage().advanceMillis(5000);

        long instance1Ts = clockOfInstance1.currentTimeMillis();
        long instance2Ts = clockOfInstance2.currentTimeMillis();
        assertNotEquals(instance1Ts, instance2Ts);

        long zeroedMsInstance1Ts = TimeUtil.zeroOutMs(instance1Ts);
        long zeroedMsInstance2Ts = TimeUtil.zeroOutMs(instance2Ts);

        IMap<Object, Object> map = instance1.getMap("TEST_MAP");
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        Set<Long> counts = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            long creationTime = map.getEntryView(i).getCreationTime();
            assertTrue(creationTime == zeroedMsInstance1Ts || creationTime == zeroedMsInstance2Ts);

            counts.add(creationTime);
        }

        assertEquals(2, counts.size());
    }


    @Test
    public void testInstancesWithSynchedClocks() {
        HazelcastInstance instance1 = instanceFactory.newHazelcastInstance();
        HazelcastInstance instance2 = instanceFactory.newHazelcastInstance();

        ManageableClock clockOfInstance1 = clockOf(instance1);
        ManageableClock clockOfInstance2 = clockOf(instance2);

        clockOfInstance1.manage().advanceMillis(1000);
        clockOfInstance2.manage().advanceMillis(5000);
        assertNotEquals(clockOfInstance1.currentTimeMillis(), clockOfInstance2.currentTimeMillis());

        clockOfInstance1.syncTo(clockOfInstance2);
        long instance1Ts = clockOfInstance1.currentTimeMillis();
        long instance2Ts = clockOfInstance2.currentTimeMillis();
        assertEquals(instance1Ts, instance2Ts);

        long zeroedMsInstance1Ts = TimeUtil.zeroOutMs(instance1Ts);

        IMap<Object, Object> map = instance1.getMap("TEST_MAP");
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        Set<Long> counts = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            long creationTime = map.getEntryView(i).getCreationTime();
            assertEquals(creationTime, zeroedMsInstance1Ts);

            counts.add(creationTime);
        }

        assertEquals(1, counts.size());
    }

}
