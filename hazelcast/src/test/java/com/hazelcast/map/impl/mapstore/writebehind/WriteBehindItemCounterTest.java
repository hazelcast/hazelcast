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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.IMap;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test is targeted to be used when {@link com.hazelcast.config.MapStoreConfig#writeCoalescing} is set false.
 * When it is false, this means we are trying to persist all updates on an entry in contrast with write-coalescing.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindItemCounterTest extends HazelcastTestSupport {

    @Test
    public void testCounter_against_one_node_zero_backup() throws Exception {
        final int maxCapacityPerNode = 100;
        final MapStoreWithCounter<Integer, Integer> mapStore = new MapStoreWithCounter<Integer, Integer>();
        final IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withWriteCoalescing(false)
                .withWriteBehindQueueCapacity(maxCapacityPerNode)
                .build();

        populateMap(map, maxCapacityPerNode);

        assertEquals(maxCapacityPerNode, map.size());
    }

    @Test
    public void testCounter_against_many_nodes() throws Exception {
        final int maxCapacityPerNode = 100;
        final int nodeCount = 2;
        final MapStoreWithCounter<Integer, Integer> mapStore = new MapStoreWithCounter<Integer, Integer>();
        final IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(nodeCount)
                .withNodeFactory(createHazelcastInstanceFactory(nodeCount))
                .withBackupCount(0)
                .withWriteCoalescing(false)
                .withWriteBehindQueueCapacity(maxCapacityPerNode)
                .withWriteDelaySeconds(100)
                .build();

        // put slightly more number of entries which is higher than max write-behind queue capacity per node
        populateMap(map, maxCapacityPerNode + 3);

        assertTrue(map.size() > maxCapacityPerNode);
    }

    @Test(expected = ReachedMaxSizeException.class)
    public void testCounter_whenMaxCapacityExceeded() {
        final int maxCapacityPerNode = 100;
        final int nodeCount = 1;
        final MapStoreWithCounter<Integer, Integer> mapStore = new MapStoreWithCounter<Integer, Integer>();
        final IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(nodeCount)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteCoalescing(false)
                .withWriteBehindQueueCapacity(maxCapacityPerNode)
                .withWriteDelaySeconds(100)
                .build();

        // exceed max write-behind queue capacity per node
        populateMap(map, 2 * maxCapacityPerNode);
    }

    private void populateMap(IMap<Integer, Integer> map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i);
        }
    }
}
