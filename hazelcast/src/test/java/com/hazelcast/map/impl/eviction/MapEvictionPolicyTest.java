/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.eviction.MapEvictionPolicy;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_PARTITION;
import static com.hazelcast.map.impl.eviction.Evictor.SAMPLE_COUNT;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.lang.String.format;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapEvictionPolicyTest extends HazelcastTestSupport {

    private final String mapName = "default";

    @Test
    public void testMapEvictionPolicy() throws Exception {
        int sampleCount = SAMPLE_COUNT;

        Config config = getConfig();
        config.setProperty(PARTITION_COUNT.getName(), "1");
        config.getMapConfig(mapName)
                .setMapEvictionPolicy(new OddEvictor())
                .getMaxSizeConfig()
                .setMaxSizePolicy(PER_PARTITION).setSize(sampleCount);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, Integer> map = instance.getMap(mapName);

        final CountDownLatch eventLatch = new CountDownLatch(1);
        final Queue<Integer> evictedKeys = new ConcurrentLinkedQueue<Integer>();
        map.addEntryListener(new EntryEvictedListener<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                evictedKeys.add(event.getKey());
                eventLatch.countDown();
            }
        }, false);

        for (int i = 0; i < sampleCount + 1; i++) {
            map.put(i, i);
        }

        assertOpenEventually("No eviction occurred", eventLatch);

        for (Integer key : evictedKeys) {
            assertTrue(format("Evicted key should be an odd number, but found %d", key), key % 2 != 0);
        }
    }

    private static class OddEvictor extends MapEvictionPolicy {

        @Override
        public int compare(EntryView o1, EntryView o2) {
            assertNotNull(o1);
            assertNotNull(o2);

            assertFalse(o1.equals(o2));

            assertTrue(o1.hashCode() != 0);
            assertTrue(o2.hashCode() != 0);

            assertNotNull(o1.toString());
            assertNotNull(o2.toString());

            Integer key = (Integer) o1.getKey();
            if (key % 2 != 0) {
                return -1;
            }

            return 1;
        }
    }
}
