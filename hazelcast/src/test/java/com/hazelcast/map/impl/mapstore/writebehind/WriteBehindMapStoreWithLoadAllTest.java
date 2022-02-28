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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindMapStoreWithLoadAllTest extends HazelcastTestSupport {

    @Test
    public void testWriteBehind_loadAll() throws Exception {
        final MapStoreWithCounter<Integer, Integer> mapStore = new MapStoreWithCounter<Integer, Integer>();
        final IMap<Integer, Integer> map = TestMapUsingMapStoreBuilder.<Integer, Integer>create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withBackupCount(0)
                .withWriteDelaySeconds(100)
                .withPartitionCount(1)
                .build();
        final int numberOfItems = 3;
        final Map<Integer, Integer> fill = new HashMap<Integer, Integer>();
        for (int i = 0; i < numberOfItems; i++) {
            fill.put(i, -1);
        }
        mapStore.storeAll(fill);
        populateMap(map, numberOfItems);
        map.loadAll(true);

        assertFinalValueEqualsForEachEntry(map, numberOfItems);
    }

    private void populateMap(IMap<Integer, Integer> map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            map.put(i, i);
        }
    }

    private void assertFinalValueEquals(final int expected, final int actual) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, actual);
            }
        }, 5);
    }

    private void assertFinalValueEqualsForEachEntry(IMap map, int numberOfItems) {
        for (int i = 0; i < numberOfItems; i++) {
            assertFinalValueEquals(i, (Integer) map.get(i));
        }
    }
}
