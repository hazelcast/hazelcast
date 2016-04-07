/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.operation.PutAllPerMemberOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapPutAllTest extends HazelcastTestSupport {

    private static final int INSTANCE_COUNT = 3;

    private static final Random RANDOM = new Random();

    @Test
    public void testPutAllPerMemberOperation_whenContainsDataForWrongPartition() throws Exception {
        String mapName = randomMapName();
        assertTrue(INSTANCE_COUNT > 2);

        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(INSTANCE_COUNT));
        config.setProperty(GroupProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        HazelcastInstance[] instances = factory.newInstances(config);
        warmUpPartitions(instances);

        HazelcastInstance hz = instances[0];
        HazelcastInstance randomHz = instances[1 + RANDOM.nextInt(INSTANCE_COUNT - 1)];
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        NodeEngineImpl randomNodeEngine = getNodeEngineImpl(randomHz);
        SerializationService serializationService = nodeEngine.getSerializationService();

        // verify that each member has a single partition
        Map<Address, List<Integer>> memberPartitionsMap = nodeEngine.getPartitionService().getMemberPartitionsMap();
        assertEquals(INSTANCE_COUNT, memberPartitionsMap.values().size());
        for (List<Integer> partitions : memberPartitionsMap.values()) {
            assertEquals(1, partitions.size());
        }

        // test that the map is empty
        Map<String, String> map = hz.getMap(mapName);
        assertEquals(0, map.size());

        // create PutAllPerMemberOperation with entries for all partitions
        int[] partitions = new int[INSTANCE_COUNT];
        MapEntries[] entries = new MapEntries[INSTANCE_COUNT];
        for (int i = 0; i < INSTANCE_COUNT; i++) {
            String key = generateKeyForPartition(hz, i);
            Data data = serializationService.toData(key);

            MapEntries mapEntries = new MapEntries();
            mapEntries.add(data, data);

            partitions[i] = i;
            entries[i] = mapEntries;
        }
        Operation op = new PutAllPerMemberOperation(mapName, partitions, entries);

        // invoke the operation on a random remote target
        final CountDownLatch latch = new CountDownLatch(1);
        InternalOperationService operationService = nodeEngine.getOperationService();
        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, op, randomNodeEngine.getThisAddress());
        future.andThen(new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                System.out.println("Got response: " + response);
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                System.out.println("Got exception: " + t);
                t.printStackTrace();
                latch.countDown();
            }
        });
        latch.await();

        // assert that the values have been inserted to the map on all instances
        for (int i = 0; i < INSTANCE_COUNT; i++) {
            map = instances[i].getMap(mapName);
            assertEquals(INSTANCE_COUNT, map.size());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                assertEquals(entry.getKey(), entry.getValue());
            }
        }
    }
}
