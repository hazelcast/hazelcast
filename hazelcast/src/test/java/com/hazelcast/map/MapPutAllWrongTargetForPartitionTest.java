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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.PutAllPartitionAwareOperationFactory;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
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

import java.util.Map;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapPutAllWrongTargetForPartitionTest extends HazelcastTestSupport {

    private static final int INSTANCE_COUNT = 3;

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] instances;

    @Before
    public void setUp() {
        assertTrue("Expected at least two members in the cluster", INSTANCE_COUNT > 2);

        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), String.valueOf(INSTANCE_COUNT));
        config.setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000");
        config.getMapConfig("default")
                .setBackupCount(1)
                .setAsyncBackupCount(0);

        factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        instances = factory.newInstances(config);
        warmUpPartitions(instances);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testPutAllPerMemberOperation_whenOperationContainsDataForAllPartitions_withSingleEntryPerPartition()
            throws Exception {
        testPutAllPerMemberOperation(1);
    }

    @Test
    public void testPutAllPerMemberOperation_whenOperationContainsDataForAllPartitions_withMultipleEntriesPerPartition()
            throws Exception {
        testPutAllPerMemberOperation(23);
    }

    /**
     * Tests that all entries and backups of a {@link PutAllPartitionAwareOperationFactory} are sent to the correct members.
     * <p>
     * The test creates a cluster with a single partition per member and invokes {@link PutAllPartitionAwareOperationFactory}
     * which contains a single entry for every partition in the cluster. So just a single entry is for the member the factory
     * is executed on.
     * <p>
     * After the operation is invoked we assert that each member owns one entry of the map and that all backups have been written.
     */
    private void testPutAllPerMemberOperation(final int entriesPerPartition) throws Exception {
        final int expectedEntryCount = INSTANCE_COUNT * entriesPerPartition;
        final String mapName = randomMapName();

        HazelcastInstance hz = instances[0];
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        SerializationService serializationService = nodeEngine.getSerializationService();

        // create a PutAllPerMemberOperation with entries for all partitions
        int[] allPartitions = getAllPartitions();
        PartitionAwareOperationFactory factory = createPutAllOperationFactory(allPartitions, entriesPerPartition, mapName, hz,
                serializationService);

        // invoke the operation on a random remote target
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        operationService.invokeOnPartitions(MapService.SERVICE_NAME, factory, allPartitions);

        // assert that all entries have been written
        IMap<String, String> map = hz.getMap(mapName);
        assertEquals(format("Expected %d entries in the map", expectedEntryCount), expectedEntryCount, map.size());
        for (Map.Entry<String, String> entry : map.entrySet()) {
            assertEquals("Expected that key and value are the same", entry.getKey(), entry.getValue());
        }

        // assert that each member owns entriesPerPartition entries of the map and that all backups have been written
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int totalBackups = 0;
                for (int i = 0; i < INSTANCE_COUNT; i++) {
                    IMap map = instances[i].getMap(mapName);
                    assertEquals(format("Each member should own %d entries of the map", entriesPerPartition),
                            entriesPerPartition, map.getLocalMapStats().getOwnedEntryCount());
                    totalBackups += map.getLocalMapStats().getBackupEntryCount();
                }
                assertEquals(format("Expected to find %d backups in the cluster", expectedEntryCount),
                        expectedEntryCount, totalBackups);
            }
        });
    }

    private int[] getAllPartitions() {
        int[] partitions = new int[INSTANCE_COUNT];
        for (int partitionId = 0; partitionId < INSTANCE_COUNT; partitionId++) {
            partitions[partitionId] = partitionId;
        }
        return partitions;
    }

    private PartitionAwareOperationFactory createPutAllOperationFactory(int[] partitions, int entriesPerPartition, String mapName,
                                                                        HazelcastInstance hz,
                                                                        SerializationService serializationService) {
        MapEntries[] entries = new MapEntries[INSTANCE_COUNT];
        for (int partitionId : partitions) {
            MapEntries mapEntries = new MapEntries(entriesPerPartition);

            for (int i = 0; i < entriesPerPartition; i++) {
                String key = generateKeyForPartition(hz, partitionId);
                Data data = serializationService.toData(key);

                mapEntries.add(data, data);
            }

            entries[partitionId] = mapEntries;
        }
        return getPutAllPartitionAwareOperationFactory(mapName, partitions, entries);
    }

    protected PartitionAwareOperationFactory getPutAllPartitionAwareOperationFactory(String mapName, int[] partitions,
                                                                                     MapEntries[] entries) {
        return new PutAllPartitionAwareOperationFactory(mapName, partitions, entries, true);
    }
}
