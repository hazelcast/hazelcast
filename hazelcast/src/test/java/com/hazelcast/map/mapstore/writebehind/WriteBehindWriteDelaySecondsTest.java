/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class WriteBehindWriteDelaySecondsTest extends HazelcastTestSupport {

    /**
     * Updates on same key should not shift store time.
     */
    @Test
    public void continuouslyUpdatedKey_shouldBeStored_inEveryWriteDelayTimeWindow() throws Exception {
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        final IMap<Object, Object> map = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(1)
                .withNodeFactory(createHazelcastInstanceFactory(1))
                .withWriteDelaySeconds(2)
                .withPartitionCount(1)
                .build();


        for (int i = 1; i <= 60; i++) {
            map.put(1, i);
            sleepMillis(100);
        }

        // min expected 1 --> due to the timing between store thread and this thread. Store thread works in every second.
        // max expected 3 --> 60 * 100 millis / 2 * 1000 millis
        assertMinMaxStoreOperationsCount(1, 3, mapStore);
    }

    private void assertMinMaxStoreOperationsCount(final int minimumExpectedStoreOperationCount,
                                                  final int maximumExpectedStoreOperationCount,
                                                  final MapStoreWithCounter mapStore) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final Object value = mapStore.store.get(1);
                final int countStore = mapStore.countStore.get();

                assertEquals(60, value);
                assertTrue("Minimum store operation count should be bigger than 1 but found = " + countStore,
                        countStore >= minimumExpectedStoreOperationCount);
                assertTrue("Maximum store operation count should be smaller than " + maximumExpectedStoreOperationCount
                                + " but found = " + countStore,
                        countStore <= maximumExpectedStoreOperationCount);
            }
        });
    }
}
