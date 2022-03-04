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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EventListenerCounterTest extends HazelcastTestSupport {

    EventListenerCounter eventListenerCounter = new EventListenerCounter();

    @Test
    public void test_getOrCreateCounter() {
        AtomicInteger counter1 = eventListenerCounter.getOrCreateCounter("A");
        AtomicInteger counter2 = eventListenerCounter.getOrCreateCounter("A");

        assertSame(counter1, counter2);
    }

    @Test
    public void test_removeCounter() {
        AtomicInteger counter1 = eventListenerCounter.getOrCreateCounter("B");
        eventListenerCounter.removeCounter("B", counter1);

        AtomicInteger counter2 = eventListenerCounter.getOrCreateCounter("B");

        assertNotSame(counter1, counter2);
    }

    @Test
    public void test_incCounter() {
        int countBefore = eventListenerCounter.getOrCreateCounter("A").get();
        eventListenerCounter.incCounter("A");
        int countAfter = eventListenerCounter.getOrCreateCounter("A").get();

        assertEquals(countBefore + 1, countAfter);
    }

    @Test
    public void test_decCounter() {
        eventListenerCounter.incCounter("A");
        int countBefore = eventListenerCounter.getOrCreateCounter("A").get();

        eventListenerCounter.decCounter("A");
        int countAfter = eventListenerCounter.getOrCreateCounter("A").get();

        assertEquals(countBefore - 1, countAfter);
    }

    @Test
    public void near_cache_invalidation_listener_does_not_cause_map_container_creation_on_join() {
        Config config = smallInstanceConfig();
        MapConfig mapConfig = config.getMapConfig("default");
        mapConfig.setNearCacheConfig(new NearCacheConfig());

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        // here a near-cache enabled map adds an
        // invalidation listener behind the scenes.
        node1.getMap("test");

        HazelcastInstance node2 = factory.newHazelcastInstance(config);
        node1.shutdown();

        assertNull("during join process, there shouldn't be any map-container created",
                getExistingMapContainer(node2, "test"));
    }

    private static MapService getMapService(HazelcastInstance node) {
        return getNodeEngineImpl(node).getService(MapService.SERVICE_NAME);
    }

    private static MapContainer getExistingMapContainer(HazelcastInstance node, String mapName) {
        MapService mapService = getMapService(node);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapContainers().get(mapName);
    }
}
