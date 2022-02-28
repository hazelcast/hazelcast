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
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindWriteBatchingTest extends HazelcastTestSupport {

    @Test
    public void testWriteBatching() throws Exception {
        final int writeBatchSize = 8;
        final MapStoreWithCounter<Integer, Integer> mapStore = new MapStoreWithCounter<Integer, Integer>();
        final IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(3)
                .withPartitionCount(1)
                .withWriteBatchSize(writeBatchSize)
                .build();

        final int numberOfItems = 1024;
        populateMap(map, numberOfItems);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                // expecting more than half of operations should have the write batch size.
                // takes this a lower bound.
                final int expectedBatchOpCount = (numberOfItems / writeBatchSize) / 2;
                final int numberOfBatchOperationsEqualWriteBatchSize
                        = mapStore.findNumberOfBatchsEqualWriteBatchSize(writeBatchSize);
                assertTrue(numberOfBatchOperationsEqualWriteBatchSize >= expectedBatchOpCount);
            }
        }, 20);
    }

    private void populateMap(IMap<Integer, Integer> map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i);
        }
    }
}
