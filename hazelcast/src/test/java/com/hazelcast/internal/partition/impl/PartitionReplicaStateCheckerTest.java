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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
        Address[] replicaAddresses = partition.getReplicaAddresses();

        partition.setReplicaAddresses(new Address[replicaAddresses.length]);

        PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();

        assertEquals(PartitionServiceState.REPLICA_NOT_SYNC, replicaStateChecker.getPartitionServiceState());

        partition.setReplicaAddresses(replicaAddresses);
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
        Address[] replicaAddresses = partition.getReplicaAddresses();

        Address[] illegalReplicaAddresses = Arrays.copyOf(replicaAddresses, replicaAddresses.length);
        Address address = new Address(replicaAddresses[0]);
        illegalReplicaAddresses[0] = new Address(address.getInetAddress(), address.getPort() + 1000);
        partition.setReplicaAddresses(illegalReplicaAddresses);

        PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();

        assertEquals(PartitionServiceState.REPLICA_NOT_SYNC, replicaStateChecker.getPartitionServiceState());

        partition.setReplicaAddresses(replicaAddresses);
        assertEquals(PartitionServiceState.SAFE, replicaStateChecker.getPartitionServiceState());
    }

    @Test
    public void shouldBeSafe_whenKnownReplicaOwnerPresent_whileNotActive() throws UnknownHostException {
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
        Address[] replicaAddresses = partition.getReplicaAddresses();

        Address[] illegalReplicaAddresses = Arrays.copyOf(replicaAddresses, replicaAddresses.length);
        Address address = new Address(replicaAddresses[0]);
        illegalReplicaAddresses[0] = new Address(address.getInetAddress(), address.getPort() + 1000);
        partition.setReplicaAddresses(illegalReplicaAddresses);

        PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();

        assertEquals(PartitionServiceState.REPLICA_NOT_SYNC, replicaStateChecker.getPartitionServiceState());

        partition.setReplicaAddresses(replicaAddresses);
        assertEquals(PartitionServiceState.SAFE, replicaStateChecker.getPartitionServiceState());
    }

    @Test
    public void shouldNotBeSafe_whenMigrationTasksScheduled() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz = factory.newHazelcastInstance();
        InternalPartitionServiceImpl partitionService = getNode(hz).partitionService;

        final CountDownLatch latch = new CountDownLatch(1);
        MigrationManager migrationManager = partitionService.getMigrationManager();
        migrationManager.schedule(new MigrationRunnable() {
            @Override
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        final PartitionReplicaStateChecker replicaStateChecker = partitionService.getPartitionReplicaStateChecker();

        assertEquals(PartitionServiceState.MIGRATION_LOCAL, replicaStateChecker.getPartitionServiceState());

        latch.countDown();
        assertEqualsEventually(new Callable<PartitionServiceState>() {
            @Override
            public PartitionServiceState call() throws Exception {
                return replicaStateChecker.getPartitionServiceState();
            }
        }, PartitionServiceState.SAFE);
    }

}
