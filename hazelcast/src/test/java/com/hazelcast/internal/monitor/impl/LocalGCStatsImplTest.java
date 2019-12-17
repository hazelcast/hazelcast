/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.memory.GarbageCollectorStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalGCStatsImplTest {

    @Test
    public void testDefaultConstructor() {
        LocalGCStatsImpl localGCStats = new LocalGCStatsImpl();
        localGCStats.setMajorCount(8);
        localGCStats.setMajorTime(7);
        localGCStats.setMinorCount(6);
        localGCStats.setMinorTime(5);
        localGCStats.setUnknownCount(4);
        localGCStats.setUnknownTime(3);

        assertTrue(localGCStats.getCreationTime() > 0);
        assertEquals(8, localGCStats.getMajorCollectionCount());
        assertEquals(7, localGCStats.getMajorCollectionTime());
        assertEquals(6, localGCStats.getMinorCollectionCount());
        assertEquals(5, localGCStats.getMinorCollectionTime());
        assertEquals(4, localGCStats.getUnknownCollectionCount());
        assertEquals(3, localGCStats.getUnknownCollectionTime());
        assertNotNull(localGCStats.toString());
    }

    @Test
    public void testSerialization() {
        GarbageCollectorStats garbageCollectorStats = new GarbageCollectorStats() {
            @Override
            public long getMajorCollectionCount() {
                return 125;
            }

            @Override
            public long getMajorCollectionTime() {
                return 14778;
            }

            @Override
            public long getMinorCollectionCount() {
                return 19;
            }

            @Override
            public long getMinorCollectionTime() {
                return 102931;
            }

            @Override
            public long getUnknownCollectionCount() {
                return 129;
            }

            @Override
            public long getUnknownCollectionTime() {
                return 49182;
            }
        };

        LocalGCStatsImpl localGCStats = new LocalGCStatsImpl(garbageCollectorStats);

        JsonObject serialized = localGCStats.toJson();
        LocalGCStatsImpl deserialized = new LocalGCStatsImpl();
        deserialized.fromJson(serialized);

        assertEquals(0, localGCStats.getCreationTime());
        assertEquals(125, localGCStats.getMajorCollectionCount());
        assertEquals(14778, localGCStats.getMajorCollectionTime());
        assertEquals(19, localGCStats.getMinorCollectionCount());
        assertEquals(102931, localGCStats.getMinorCollectionTime());
        assertEquals(129, localGCStats.getUnknownCollectionCount());
        assertEquals(49182, localGCStats.getUnknownCollectionTime());
        assertNotNull(localGCStats.toString());
    }
}
