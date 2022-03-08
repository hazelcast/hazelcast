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

package com.hazelcast.map.merge;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMapAccessors;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.SplitBrainTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class MapInternalStateSplitBrainTest extends SplitBrainTestSupport {

    private static final int[] BRAINS = new int[]{3, 2};

    public InMemoryFormat inMemoryFormat;

    private static String[] mapNames = {"mapNameA", "mapNameB"};

    private MergeLifecycleListener mergeLifecycleListener;
    private Map<String, List<MapContainer>> containersInitial = new HashMap<>();
    private Map<String, List<MapContainer>> containersInFirstBrain = new HashMap<>();
    private Map<String, List<MapContainer>> containersInSecondBrain = new HashMap<>();

    @Override
    protected int[] brains() {
        return BRAINS;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HazelcastInstance[] instances) {
        waitAllForSafeState(instances);

        // create 2 maps internal data structures
        for (String mapName : mapNames) {
            instances[0].getMap(mapName).size();
        }

        // collect mapContainer objects
        containersInitial = collectContainersFrom(instances);
    }

    @Override
    protected void onAfterSplitBrainCreated(HazelcastInstance[] firstBrain, HazelcastInstance[] secondBrain) {
        mergeLifecycleListener = new MergeLifecycleListener(secondBrain.length);
        for (HazelcastInstance instance : secondBrain) {
            instance.getLifecycleService().addLifecycleListener(mergeLifecycleListener);
        }

        containersInFirstBrain = collectContainersFrom(firstBrain);
        containersInSecondBrain = collectContainersFrom(secondBrain);
    }

    @Override
    protected void onAfterSplitBrainHealed(HazelcastInstance[] instances) {
        // wait until merge completes
        mergeLifecycleListener.await();

        // force create 2 maps internal data structures if not exist before.
        for (String mapName : mapNames) {
            instances[0].getMap(mapName).size();
        }

        Map<String, List<MapContainer>> containersAfterMerge = collectContainersFrom(instances);

        for (String mapName : mapNames) {
            List<MapContainer> firstBrainContainers = containersInFirstBrain.get(mapName);
            List<MapContainer> secondBrainContainers = containersInSecondBrain.get(mapName);

            List<MapContainer> initialContainers = containersInitial.get(mapName);
            List<MapContainer> afterMergeContainers = containersAfterMerge.get(mapName);

            // after split, we expect we still have same mapContainer
            // objects with initial ones in both brains
            assertTrue(initialContainers.containsAll(firstBrainContainers));
            assertTrue(initialContainers.containsAll(secondBrainContainers));

            // after heal we expect mapContainers in bigger brains still with us
            assertTrue(afterMergeContainers.containsAll(firstBrainContainers));
            assertFalse(afterMergeContainers.containsAll(secondBrainContainers));
        }
    }

    private static Map<String, List<MapContainer>> collectContainersFrom(HazelcastInstance[] instances) {
        Map<String, List<MapContainer>> containers = new HashMap<>();

        for (String mapName : mapNames) {
            for (HazelcastInstance instance : instances) {
                MapContainer container = IMapAccessors.getMapContainer(instance.getMap(mapName));
                containers.computeIfAbsent(mapName, s -> new ArrayList<>()).add(container);
            }
        }

        return containers;
    }
}
