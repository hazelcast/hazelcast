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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static com.hazelcast.test.Accessors.getPartitionService;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapMigrationRecordStoreExistenceTest extends HazelcastTestSupport {

    @Test
    public void testRecordStoreExistence() {
        String mapName = UuidUtil.newUnsecureUuidString();
        Config config = getConfig();
        config.addMapConfig(new MapConfig(mapName)
                .setBackupCount(1)
                .setAsyncBackupCount(0));

        // setup cluster
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        int partitionCount = getPartitionService(instance1).getPartitionCount();

        waitAllForSafeState(instance1, instance2);

        // populate map to create the record stores
        Map<Integer, Integer> map = instance1.getMap(mapName);
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        // add instance3 to get some partitions removed from every member
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(config);

        waitAllForSafeState(instance1, instance2, instance3);

        try {
            for (HazelcastInstance instance : new HazelcastInstance[]{instance1, instance2, instance3}) {
                Address instanceAddress = Accessors.getAddress(instance);
                InternalPartitionService partitionService = getPartitionService(instance);

                MapService mapService = Accessors.getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
                MapServiceContext mapServiceContext = mapService.getMapServiceContext();

                for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                    InternalPartition partition = partitionService.getPartition(partitionId);

                    PartitionReplica primaryReplica = partition.getReplica(0);
                    PartitionReplica backupReplica = partition.getReplica(1);

                    boolean primary = primaryReplica.address().equals(instanceAddress);
                    boolean backup = backupReplica.address().equals(instanceAddress);

                    RecordStore<?> recordStore = mapServiceContext.getExistingRecordStore(partitionId, mapName);

                    if (primary || backup) {
                        assertNotNull("Address/partition " + instanceAddress + "/" + partitionId, recordStore);
                    } else {
                        assertNull("Address/partition " + instanceAddress + "/" + partitionId, recordStore);
                    }
                }
            }
        } finally {
            nodeFactory.terminateAll();
        }
    }
}
