/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapPutAllWithBatchingTest extends HazelcastTestSupport {

    private static final int INSTANCE_COUNT = 2;
    private static final int BATCH_SIZE = 10;

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] instances;

    @Parameterized.Parameter
    public boolean async;

    @Parameterized.Parameters(name = "async={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[]{false},
                new Object[]{true}
        );
    }

    @Before
    public void setUp() {
        Config config = getConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), valueOf(INSTANCE_COUNT * 2))
                .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000")
                .setProperty("hazelcast.map.put.all.batch.size", valueOf(BATCH_SIZE));

        config.getMapConfig("default")
                .setBackupCount(1)
                .setAsyncBackupCount(0);

        factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        instances = factory.newInstances(config);
        warmUpPartitions(instances);
    }

    @Test
    public void testPutAll() throws Exception {
        testPutAll(1 + BATCH_SIZE * INSTANCE_COUNT * 2);
    }

    @Test
    public void testPutAll_withSingleMapEntry() throws Exception {
        testPutAll(1);
    }

    @Test
    public void testPutAll_withEmptyMap() throws Exception {
        testPutAll(0);
    }

    private void testPutAll(int expectedEntryCount) throws Exception {
        String mapName = randomMapName();
        HazelcastInstance hz = instances[0];

        Map<Integer, Integer> inputMap = new HashMap<Integer, Integer>(expectedEntryCount);
        for (int i = 0; i < expectedEntryCount; i++) {
            inputMap.put(i, i);
        }

        // assert that the map is empty
        IMap<Integer, Integer> map = hz.getMap(mapName);
        assertEquals("Expected an empty map", 0, map.size());

        if (async) {
            map.putAllAsync(inputMap).toCompletableFuture().get(15, TimeUnit.SECONDS);
        } else {
            map.putAll(inputMap);
        }

        // assert that all entries have been written
        assertEquals(format("Expected %d entries in the map", expectedEntryCount), expectedEntryCount, map.size());
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            assertEquals("Expected that key and value are the same", entry.getKey(), entry.getValue());
        }
    }
}
