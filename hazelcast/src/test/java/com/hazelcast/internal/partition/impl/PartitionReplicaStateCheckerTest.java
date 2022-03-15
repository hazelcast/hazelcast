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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.service.TestMigrationAwareService;
import com.hazelcast.internal.partition.service.TestPutOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.internal.partition.AntiEntropyCorrectnessTest.setBackupPacketDropFilter;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionReplicaStateCheckerTest extends HazelcastTestSupport {

    @Test
    public void shouldBeSafe_whenNotInitialized() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;

        PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();
        PartitionServiceState state = replicaStateChecker.getPartitionServiceState();
        assertEquals(PartitionServiceState.SAFE, state);
    }

    @Test
    public void shouldBeSafe_whenInitializedOnMaster() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;
        partitionService.firstArrangement();

        PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();
        PartitionServiceState state = replicaStateChecker.getPartitionServiceState();
        assertEquals(PartitionServiceState.SAFE, state);
    }

    @Test
    public void shouldNotBeSafe_whenMissingReplicasPresent() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;
        partitionService.firstArrangement();

        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(0);
        PartitionReplica[] members = partition.replicas();

        partition.setReplicas(new PartitionReplica[members.length]);

        PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();

        assertEquals(PartitionServiceState.REPLICA_NOT_OWNED, replicaStateChecker.getPartitionServiceState());

        partition.setReplicas(members);
        assertEquals(PartitionServiceState.SAFE, replicaStateChecker.getPartitionServiceState());
    }

    @Test
    public void shouldNotBeSafe_whenUnknownReplicaOwnerPresent() throws UnknownHostException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;
        partitionService.firstArrangement();

        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(0);
        PartitionReplica[] members = partition.replicas();

        PartitionReplica[] illegalMembers = Arrays.copyOf(members, members.length);
        Address address = members[0].address();
        illegalMembers[0] = new PartitionReplica(
                new Address(address.getInetAddress(), address.getPort() + 1000), members[0].uuid());
        partition.setReplicas(illegalMembers);

        PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();

        assertEquals(PartitionServiceState.REPLICA_NOT_OWNED, replicaStateChecker.getPartitionServiceState());

        partition.setReplicas(members);
        assertEquals(PartitionServiceState.SAFE, replicaStateChecker.getPartitionServiceState());
    }

    @Test
    public void shouldBeSafe_whenKnownReplicaOwnerPresent_whileNotActive() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;
        partitionService.firstArrangement();

        changeClusterStateEventually(hz2, ClusterState.FROZEN);

        hz2.shutdown();
        assertClusterSizeEventually(1, hz);

        PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();
        assertEquals(PartitionServiceState.SAFE, replicaStateChecker.getPartitionServiceState());
    }

    @Test
    public void shouldNotBeSafe_whenUnknownReplicaOwnerPresent_whileNotActive() throws UnknownHostException {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        HazelcastInstance hz2 = factory.newHazelcastInstance();

        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;
        partitionService.firstArrangement();

        changeClusterStateEventually(hz2, ClusterState.FROZEN);

        hz2.shutdown();
        assertClusterSizeEventually(1, hz);

        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(0);
        PartitionReplica[] members = partition.replicas();

        PartitionReplica[] illegalMembers = Arrays.copyOf(members, members.length);
        Address address = members[0].address();
        illegalMembers[0] = new PartitionReplica(new Address(address.getInetAddress(), address.getPort() + 1000), members[0].uuid());
        partition.setReplicas(illegalMembers);

        PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();

        assertEquals(PartitionServiceState.REPLICA_NOT_OWNED, replicaStateChecker.getPartitionServiceState());

        partition.setReplicas(members);
        assertEquals(PartitionServiceState.SAFE, replicaStateChecker.getPartitionServiceState());
    }

    @Test
    public void shouldNotBeSafe_whenMigrationTasksScheduled() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;

        final CountDownLatch latch = new CountDownLatch(1);
        MigrationManager migrationManager = partitionService.getMigrationManager();
        migrationManager.schedule(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();

        assertEquals(PartitionServiceState.MIGRATION_LOCAL, replicaStateChecker.getPartitionServiceState());

        latch.countDown();
        assertEqualsEventually(replicaStateChecker::getPartitionServiceState, PartitionServiceState.SAFE);
    }

    @Test
    public void shouldNotBeSafe_whenReplicasAreNotSync() {
        Config config = new Config();
        ServiceConfig serviceConfig = TestMigrationAwareService.createServiceConfig(1);
        ConfigAccessor.getServicesConfig(config).addServiceConfig(serviceConfig);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance(config);
        HazelcastInstance hz2 = factory.newHazelcastInstance(config);

        InternalPartitionServiceImpl partitionService1 = getNode(hz).partitionService;
        InternalPartitionServiceImpl partitionService2 = getNode(hz2).partitionService;
        int maxPermits = drainAllReplicaSyncPermits(partitionService1);
        int maxPermits2 = drainAllReplicaSyncPermits(partitionService2);
        assertEquals(maxPermits, maxPermits2);

        warmUpPartitions(hz, hz2);

        setBackupPacketDropFilter(hz, 100);
        setBackupPacketDropFilter(hz2, 100);

        NodeEngine nodeEngine = getNode(hz).nodeEngine;
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            Operation op = new TestPutOperationWithAsyncBackup(i);
            nodeEngine.getOperationService().invokeOnPartition(null, op, i).join();
        }

        final PartitionReplicaStateChecker replicaStateChecker1 = partitionService1.getPartitionReplicaStateChecker();
        final PartitionReplicaStateChecker replicaStateChecker2 = partitionService2.getPartitionReplicaStateChecker();

        assertEquals(PartitionServiceState.REPLICA_NOT_SYNC, replicaStateChecker1.getPartitionServiceState());
        assertEquals(PartitionServiceState.REPLICA_NOT_SYNC, replicaStateChecker2.getPartitionServiceState());

        addReplicaSyncPermits(partitionService1, maxPermits);
        addReplicaSyncPermits(partitionService2, maxPermits);

        assertTrueEventually(() -> {
            assertEquals(PartitionServiceState.SAFE, replicaStateChecker1.getPartitionServiceState());
            assertEquals(PartitionServiceState.SAFE, replicaStateChecker2.getPartitionServiceState());
        });
    }

    private void addReplicaSyncPermits(InternalPartitionServiceImpl partitionService, int k) {
        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        replicaManager.releaseReplicaSyncPermits(k);
    }

    private int drainAllReplicaSyncPermits(InternalPartitionServiceImpl partitionService) {
        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        return replicaManager.tryAcquireReplicaSyncPermits(Integer.MAX_VALUE);
    }

    private static class TestPutOperationWithAsyncBackup extends TestPutOperation {

        TestPutOperationWithAsyncBackup() {
        }

        TestPutOperationWithAsyncBackup(int i) {
            super(i);
        }

        @Override
        public int getSyncBackupCount() {
            return 0;
        }

        @Override
        public int getAsyncBackupCount() {
            return super.getSyncBackupCount();
        }
    }
}
