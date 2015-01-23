/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test is targeted to be used when {@link com.hazelcast.config.MapStoreConfig#writeCoalescing} is set false.
 * When it is false, this means we are trying to store all updates on an entry in contrast with write-coalescing.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class WriteBehindStoreAllUpdatesTest extends HazelcastTestSupport {

    @Test
    public void testAllUpdatesReflectedToMapStore() throws Exception {
        int nodeCount = 3;
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        TestMapUsingMapStoreBuilder builder = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(nodeCount)
                .withNodeFactory(createHazelcastInstanceFactory(nodeCount))
                .withBackupCount(0)
                .withWriteCoalescing(false)
                .withWriteDelaySeconds(3);
        IMap<Object, Object> map = builder.build();

        for (int i = 0; i < 500; i++) {
            map.put(i, randomString());
        }

        for (int i = 0; i < 500; i++) {
            map.remove(i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                final int storeCount = mapStore.countStore.get();
                final int deleteCount = mapStore.countDelete.get();

                assertEquals(1000, storeCount + deleteCount);
                assertTrue(mapStore.store.isEmpty());
            }
        });
    }
}