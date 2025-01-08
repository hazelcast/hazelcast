/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionReplicaSyncRequestOffloadableTest extends HazelcastTestSupport {

    @Test
    public void whenOperationNotComplete_migratingFlagIsSet() {
        final HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        final NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        final InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        final int partitionId = 0;
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);

        final Operation operation = new PartitionReplicaSyncRequestOffloadable(
                Collections.singleton(new DistributedObjectNamespace(MapService.SERVICE_NAME, "someMap")),
                partitionId, 1);

        var future = nodeEngineImpl.getOperationService().invokeOnTarget(
                IPartitionService.SERVICE_NAME, operation, nodeEngineImpl.getThisAddress());

        assertTrueEventually(() -> assertThat(partition.isMigrating()).as("Migrating flag should be set before completion").isTrue());
        assertThat(future).as("Operation should still be in progress").isNotCompleted();
    }

    @Test
    public void whenOperationComplete_migratingFlagIsNotSet() {
        final HazelcastInstance[] instances = createHazelcastInstances(smallInstanceConfig(), 2);
        final HazelcastInstance instance = instances[0];
        // need object and partitions assignment
        warmUpPartitions(instances);
        waitAllForSafeState(instances);
        instance.getMap("someMap");

        final NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        final InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        // we will send PartitionReplicaSyncRequestOffloadable from instance[0] to instance [1]
        // it has to be for partition owned by instance[1]
        final int partitionId = getPartitionId(instances[1]);
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);

        final Operation operation = new PartitionReplicaSyncRequestOffloadable(
                Collections.singleton(new DistributedObjectNamespace(MapService.SERVICE_NAME, "someMap")),
                partitionId, 1);

        assertCompletesEventually(nodeEngineImpl.getOperationService().invokeOnTarget(
                IPartitionService.SERVICE_NAME, operation, partition.getOwnerOrNull()));

        assertThat(partition.isMigrating()).as("Migrating flag should not be set after completion").isFalse();
    }

    @Test
    public void whenNoPermits_migratingFlagIsNotSet() {
        final HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        final NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        final InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        final int partitionId = 0;
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);

        ((InternalPartitionServiceImpl) partitionService).getReplicaManager()
                .tryAcquireReplicaSyncPermits(Integer.parseInt(PARTITION_MAX_PARALLEL_REPLICATIONS.getDefaultValue()));

        // formally correct operation which should finish early due to lack of permits and result in retry
        final Operation operation = new PartitionReplicaSyncRequestOffloadable(
                Collections.singleton(new DistributedObjectNamespace(MapService.SERVICE_NAME, "someMap")),
                partitionId, 1);

        assertCompletesEventually(nodeEngineImpl.getOperationService().invokeOnTarget(
                IPartitionService.SERVICE_NAME, operation, nodeEngineImpl.getThisAddress()));

        assertThat(partition.isMigrating()).as("Migrating flag should not be set after completion").isFalse();
    }

    @Test
    public void whenPartitionIsMigrating_migratingFlagNotCleared() {
        final HazelcastInstance instance = createHazelcastInstance(smallInstanceConfig());
        final NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
        final InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        final int partitionId = 0;
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);
        partition.setMigrating();

        // not strictly needed for this test but triggers subsequent early termination condition
        // so it does not wait very long
        ((InternalPartitionServiceImpl) partitionService).getReplicaManager()
                .tryAcquireReplicaSyncPermits(Integer.parseInt(PARTITION_MAX_PARALLEL_REPLICATIONS.getDefaultValue()));

        // formally correct operation which should finish early
        final Operation operation = new PartitionReplicaSyncRequestOffloadable(
                Collections.singleton(new DistributedObjectNamespace(MapService.SERVICE_NAME, "someMap")),
                partitionId, 1);

        assertCompletesEventually(nodeEngineImpl.getOperationService().invokeOnTarget(
                IPartitionService.SERVICE_NAME, operation, nodeEngineImpl.getThisAddress()));

        assertThat(partition.isMigrating()).as("Migrating flag should not be cleared").isTrue();
    }
}
