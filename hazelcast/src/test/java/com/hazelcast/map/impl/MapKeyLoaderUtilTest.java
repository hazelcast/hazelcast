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

package com.hazelcast.map.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static com.hazelcast.map.impl.MapKeyLoader.Role;
import static com.hazelcast.map.impl.MapKeyLoader.Role.NONE;
import static com.hazelcast.map.impl.MapKeyLoader.Role.RECEIVER;
import static com.hazelcast.map.impl.MapKeyLoader.Role.SENDER;
import static com.hazelcast.map.impl.MapKeyLoader.Role.SENDER_BACKUP;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("ConstantConditions")
public class MapKeyLoaderUtilTest extends HazelcastTestSupport {

    @Test
    public void assignRole_SENDER() {
        boolean isPartitionOwner = true;
        boolean isMapNamePartition = true;
        boolean isMapNamePartitionFirstReplica = false;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, isMapNamePartitionFirstReplica);

        assertEquals(SENDER, role);
    }

    @Test
    public void assignRole_SENDER_BACKUP() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = true;
        boolean isMapNamePartitionFirstReplica = true;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, isMapNamePartitionFirstReplica);

        assertEquals(SENDER_BACKUP, role);
    }

    @Test
    public void assignRole_NOT_SENDER_BACKUP() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = true;
        boolean isMapNamePartitionFirstReplica = false;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, isMapNamePartitionFirstReplica);

        assertEquals(NONE, role);
    }

    @Test
    public void assignRole_RECEIVER_insignificantFlagFalse() {
        boolean isPartitionOwner = true;
        boolean isMapNamePartition = false;
        boolean insignificant = false;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(RECEIVER, role);
    }

    @Test
    public void assignRole_RECEIVER_insignificantFlagTrue() {
        boolean isPartitionOwner = true;
        boolean isMapNamePartition = false;
        boolean insignificant = true;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(RECEIVER, role);
    }

    @Test
    public void assignRole_NONE_insignificantFlagFalse() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = false;
        boolean insignificant = false;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(NONE, role);
    }

    @Test
    public void assignRole_NONE_insignificantFlagTrue() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = false;
        boolean insignificant = true;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(NONE, role);
    }

    @Test
    public void assignRole_NONE_impossibleCombination() {
        boolean isPartitionOwner = false;
        boolean isMapNamePartition = false;
        boolean insignificant = true;

        Role role = MapKeyLoaderUtil.assignRole(isPartitionOwner, isMapNamePartition, insignificant);

        assertEquals(NONE, role);
    }

    @Test
    public void test_toBatches_with_nodeWideLimit() {
        int nodeWideLimit = 7;
        int entryCount = 100;

        Semaphore nodeWideLoadedKeyLimiter = new Semaphore(nodeWideLimit);
        Iterator<Map<Integer, List<Data>>> batches
                = MapKeyLoaderUtil.toBatches(newIterator(entryCount),
                1000, nodeWideLoadedKeyLimiter);

        List<Integer> batchSizes = new ArrayList<>();
        while (batches.hasNext()) {
            Map<Integer, List<Data>> batch = batches.next();
            batchSizes.add(batch.size());
            nodeWideLoadedKeyLimiter.release(batch.values().size());
        }

        // assert no batch is bigger than nodeWideLimit.
        assertEquals(0,
                batchSizes.stream().filter(size -> size > nodeWideLimit).count());
        // assert sum of all batches equals to entryCount.
        assertEquals(entryCount,
                batchSizes.stream().mapToInt(size -> size).sum());

    }

    private static Iterator<Map.Entry<Integer, Data>> newIterator(int entryCount) {
        Map<Integer, Data> store = new HashMap<>();
        for (int i = 0; i < entryCount; i++) {
            store.put(i, new HeapData());
        }

        return store.entrySet().iterator();
    }
}
