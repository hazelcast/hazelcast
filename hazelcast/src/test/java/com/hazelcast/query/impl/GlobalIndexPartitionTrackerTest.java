/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.query.impl.GlobalIndexPartitionTracker.PartitionStamp;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GlobalIndexPartitionTrackerTest {
    @Test
    public void test_index_unindex() {
        GlobalIndexPartitionTracker tracker = new GlobalIndexPartitionTracker(100);

        for (int i = 0; i < 100; i++) {
            assertFalse(tracker.isIndexed(i));
        }

        tracker.beginPartitionUpdate();
        tracker.partitionIndexed(10);
        assertTrue(tracker.isIndexed(10));
        assertEquals(1, tracker.indexedCount());

        tracker.beginPartitionUpdate();
        tracker.partitionIndexed(11);
        assertTrue(tracker.isIndexed(11));
        assertEquals(2, tracker.indexedCount());

        tracker.beginPartitionUpdate();
        tracker.partitionUnindexed(10);
        assertFalse(tracker.isIndexed(10));
        assertEquals(1, tracker.indexedCount());

        tracker.beginPartitionUpdate();
        tracker.partitionUnindexed(11);
        assertFalse(tracker.isIndexed(11));
        assertEquals(0, tracker.indexedCount());
    }

    @Test
    public void test_clear() {
        GlobalIndexPartitionTracker tracker = new GlobalIndexPartitionTracker(100);

        for (int i = 0; i < 100; i++) {
            tracker.beginPartitionUpdate();
            tracker.partitionIndexed(i);
        }

        assertEquals(100, tracker.indexedCount());

        tracker.clear();

        assertEquals(0, tracker.indexedCount());
    }

    @Test
    public void test_stamp() {
        int count = 100;

        GlobalIndexPartitionTracker tracker = new GlobalIndexPartitionTracker(count);

        PartitionStamp stamp1 = tracker.getPartitionStamp();
        assertEquals(new PartitionStamp(0, partitions(count)), stamp1);
        assertTrue(tracker.validatePartitionStamp(stamp1.stamp));

        tracker.beginPartitionUpdate();
        assertNull(tracker.getPartitionStamp());

        tracker.partitionIndexed(1);
        PartitionStamp stamp2 = tracker.getPartitionStamp();
        assertNotNull(stamp2);
        assertEquals(stamp2.partitions, partitions(count, 1));
        assertTrue(stamp2.stamp > stamp1.stamp);
        assertFalse(tracker.validatePartitionStamp(stamp1.stamp));
        assertTrue(tracker.validatePartitionStamp(stamp2.stamp));

        tracker.clear();
        PartitionStamp stamp3 = tracker.getPartitionStamp();
        assertNotNull(stamp3);
        assertEquals(stamp3.partitions, partitions(count));
        assertTrue(stamp3.stamp > stamp2.stamp);
        assertFalse(tracker.validatePartitionStamp(stamp1.stamp));
        assertFalse(tracker.validatePartitionStamp(stamp2.stamp));
        assertTrue(tracker.validatePartitionStamp(stamp3.stamp));
    }

    private static PartitionIdSet partitions(int partitionCount, int... partitions) {
        PartitionIdSet res = new PartitionIdSet(partitionCount);

        if (partitions != null) {
            for (int partition : partitions) {
                res.add(partition);
            }
        }

        return res;
    }
}
