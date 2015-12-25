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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindOnBackupsTest.writeBehindQueueSize;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WriteBehindFlushTest extends HazelcastTestSupport {

    @Test
    public void testWriteBehindQueues_flushed_onNodeShutdown() throws Exception {
        int nodeCount = 3;
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        TestMapUsingMapStoreBuilder builder = TestMapUsingMapStoreBuilder.create()
                .withMapStore(mapStore)
                .withNodeCount(nodeCount)
                .withNodeFactory(factory)
                .withConfig(getConfig())
                .withWriteDelaySeconds(300);
        IMap map = builder.build();

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        factory.shutdownAll();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1000, mapStore.countStore.get());
            }
        });
    }

    @Test
    public void testWriteBehindQueues_emptied_onBackupNodes() throws Exception {
        int nodeCount = 3;
        String mapName = randomMapName();
        final MapStoreWithCounter mapStore = new MapStoreWithCounter<Integer, String>();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        TestMapUsingMapStoreBuilder builder = TestMapUsingMapStoreBuilder.create()
                .mapName(mapName)
                .withMapStore(mapStore)
                .withNodeCount(nodeCount)
                .withBackupCount(1)
                .withConfig(getConfig())
                .withNodeFactory(factory)
                .withWriteDelaySeconds(300);
        IMap map = builder.build();

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        map.flush();

        assertWriteBehindQueuesEmpty(mapName, builder.getNodes());
    }

    protected void assertWriteBehindQueuesEmpty(String mapName, HazelcastInstance[] nodes) {
        for (HazelcastInstance instance : nodes) {
            assertEquals(0, writeBehindQueueSize(instance, mapName));
        }
    }
}
