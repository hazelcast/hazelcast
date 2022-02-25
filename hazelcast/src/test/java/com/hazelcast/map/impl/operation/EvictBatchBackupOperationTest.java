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

package com.hazelcast.map.impl.operation;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EvictBatchBackupOperationTest extends HazelcastTestSupport {

    TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void operation_evicts_all_replicas() {
        // 1. Create config
        int backupCount = 2;
        final String mapName = "test";

        Config config = getConfig();
        config.getMapConfig(mapName).setBackupCount(backupCount);

        // 2. Create nodes
        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        HazelcastInstance node2 = factory.newHazelcastInstance(config);
        HazelcastInstance node3 = factory.newHazelcastInstance(config);

        // 3. Populate replicas
        IMap map = node1.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.set(i, i);
        }

        // 4. Evict all replicas
        int partitionCount = getPartitionService(node1).getPartitionCount();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            for (int replicaIndex = 0; replicaIndex <= backupCount; replicaIndex++) {
                EvictBatchBackupOperation operation = new EvictBatchBackupOperation(mapName,
                        Collections.<ExpiredKey>emptyList(), 0);
                OperationServiceImpl operationService = getOperationService(node1);
                operationService.createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId)
                        .setReplicaIndex(replicaIndex).invoke().join();
            }
        }

        // 5. All replicas should be empty in the end.
        for (HazelcastInstance node : factory.getAllHazelcastInstances()) {
            assertEquals(0, sumOwnedAndBackupEntryCount(node.getMap(mapName).getLocalMapStats()));
        }
    }

    private static long sumOwnedAndBackupEntryCount(LocalMapStats localMapStats) {
        long ownedEntryCount = localMapStats.getOwnedEntryCount();
        long backupEntryCount = localMapStats.getBackupEntryCount();
        return ownedEntryCount + backupEntryCount;
    }
}
