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

package com.hazelcast.multimap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MultiMap;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiMapContainerStatisticsTest extends HazelcastTestSupport {

    private String key;
    private MultiMap<String, String> multiMap;
    private MultiMapContainer mapContainer;

    private long previousAccessTime;
    private long previousUpdateTime;

    @Before
    public void setUp() {
        HazelcastInstance hz = createHazelcastInstance(smallInstanceConfig());
        key = generateKeyForPartition(hz, 0);

        NodeEngineImpl nodeEngine = HazelcastTestSupport.getNodeEngineImpl(hz);
        MultiMapService mapService = nodeEngine.getService(MultiMapService.SERVICE_NAME);

        multiMap = hz.getMultiMap("multiMap");
        mapContainer = mapService.getOrCreateCollectionContainerWithoutAccess(0, "multiMap");

        previousAccessTime = mapContainer.getLastAccessTime();
        previousUpdateTime = mapContainer.getLastUpdateTime();
    }

    @Test
    public void testMultiMapContainerStats() {
        assertNotEqualsStringFormat("Expected the creationTime not to be %d, but was %d", 0L, mapContainer.getCreationTime());
        assertEqualsStringFormat("Expected the lastAccessTime to be %d, but was %d", 0L, mapContainer.getLastAccessTime());
        assertEqualsStringFormat("Expected the lastUpdateTime to be %d, but was %d", 0L, mapContainer.getLastUpdateTime());

        // a get operation updates the lastAccessTime, but not the lastUpdateTime
        sleepMillis(10);
        multiMap.get(key);
        assertNewLastAccessTime();
        assertSameLastUpdateTime();

        // a put operation updates the lastAccessTime and lastUpdateTime
        sleepMillis(10);
        multiMap.put(key, "value");
        assertNewLastAccessTime();
        assertNewLastUpdateTime();

        // a get operation updates the lastAccessTime, but not the lastUpdateTime
        sleepMillis(10);
        multiMap.get(key);
        assertNewLastAccessTime();
        assertSameLastUpdateTime();

        // a delete operation updates the lastAccessTime and lastUpdateTime
        sleepMillis(10);
        multiMap.delete(key);
        assertNewLastAccessTime();
        assertNewLastUpdateTime();

        // a put operation updates the lastAccessTime and lastUpdateTime
        sleepMillis(10);
        multiMap.put(key, "value");
        assertNewLastAccessTime();
        assertNewLastUpdateTime();

        // an unsuccessful remove operation updates the lastAccessTime, but not the lastUpdateTime
        sleepMillis(10);
        assertFalse("Expected an unsuccessful remove operation", multiMap.remove(key, "invalidValue"));
        assertNewLastAccessTime();
        assertSameLastUpdateTime();

        // a successful remove operation updates the lastAccessTime and the lastUpdateTime
        sleepMillis(10);
        assertTrue("Expected a successful remove operation", multiMap.remove(key, "value"));
        assertNewLastAccessTime();
        assertNewLastUpdateTime();

        // an unsuccessful clear operation updates the lastAccessTime, but not the lastUpdateTime
        sleepMillis(10);
        multiMap.clear();
        assertNewLastAccessTime();
        assertSameLastUpdateTime();

        // a put operation updates the lastAccessTime and lastUpdateTime
        sleepMillis(10);
        multiMap.put(key, "value");
        assertNewLastAccessTime();
        assertNewLastUpdateTime();

        // a successful clear operation updates the lastAccessTime and the lastUpdateTime
        sleepMillis(10);
        multiMap.clear();
        assertNewLastAccessTime();
        assertNewLastUpdateTime();
    }

    private void assertNewLastAccessTime() {
        long lastAccessTime = mapContainer.getLastAccessTime();
        assertTrue(format("Expected the lastAccessTime %d to be higher than the previousAccessTime %d (diff: %d ms)",
                lastAccessTime, previousAccessTime, lastAccessTime - previousAccessTime),
                lastAccessTime > previousAccessTime);
        previousAccessTime = lastAccessTime;
    }

    private void assertSameLastUpdateTime() {
        long lastUpdateTime = mapContainer.getLastUpdateTime();
        assertEqualsStringFormat("Expected the lastUpdateTime to be %d, but was %d", previousUpdateTime, lastUpdateTime);
        previousUpdateTime = lastUpdateTime;
    }

    private void assertNewLastUpdateTime() {
        long lastUpdateTime = mapContainer.getLastUpdateTime();
        assertTrue(format("Expected the lastUpdateTime %d to be higher than the previousAccessTime %d (diff: %d ms)",
                lastUpdateTime, previousUpdateTime, lastUpdateTime - previousUpdateTime),
                lastUpdateTime > previousUpdateTime);
        previousUpdateTime = lastUpdateTime;
    }
}
