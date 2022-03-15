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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapPutAllWithCustomInitialSizeTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] instances;

    @Before
    public void setUp() {
        Config config = getConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2")
                .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000")
                .setProperty("hazelcast.map.put.all.initial.size.factor", "10");

        config.getMapConfig("default")
                .setBackupCount(1)
                .setAsyncBackupCount(0);

        factory = createHazelcastInstanceFactory(1);
        instances = factory.newInstances(config);
        warmUpPartitions(instances);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testPutAll() {
        testPutAll(20);
    }

    @Test
    public void testPutAll_withSingleMapEntry() {
        testPutAll(1);
    }

    @Test
    public void testPutAll_withEmptyMap() {
        testPutAll(0);
    }

    private void testPutAll(int expectedEntryCount) {
        String mapName = randomMapName();
        HazelcastInstance hz = instances[0];

        Map<Integer, Integer> inputMap = new HashMap<Integer, Integer>(expectedEntryCount);
        for (int i = 0; i < expectedEntryCount; i++) {
            inputMap.put(i, i);
        }

        // assert that the map is empty
        IMap<Integer, Integer> map = hz.getMap(mapName);
        assertEquals("Expected an empty map", 0, map.size());

        map.putAll(inputMap);

        // assert that all entries have been written
        assertEquals(format("Expected %d entries in the map", expectedEntryCount), expectedEntryCount, map.size());
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            assertEquals("Expected that key and value are the same", entry.getKey(), entry.getValue());
        }
    }
}
