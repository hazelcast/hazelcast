package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.memory.GarbageCollectorStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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
