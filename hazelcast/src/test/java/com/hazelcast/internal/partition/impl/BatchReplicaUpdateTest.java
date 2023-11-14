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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.services.CoreService;
import com.hazelcast.internal.services.PreJoinAwareService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.partition.impl.BatchReplicaUpdateTest.InjectInterceptorOp.REPLICA_UPDATE_COUNTER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchReplicaUpdateTest {

    TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory();

    @AfterEach
    void tearDown() {
        factory.terminateAll();
        REPLICA_UPDATE_COUNTER.reset();
    }

    @Test
    void testFirstArrangement() {
        // configure member with high partition count
        Config config = HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "" + 20000);
        HazelcastInstance member = factory.newHazelcastInstance(config);
        // setup interceptor
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) Accessors.getPartitionService(member);
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        ReplicaUpdateCounter counter = new ReplicaUpdateCounter();
        partitionStateManager.setReplicaUpdateInterceptor(counter);
        // trigger first arrangement of partitions
        partitionStateManager.initializePartitionAssignments(Collections.emptySet());
        assertEquals(1, counter.batchReplicaCounter.get());
        assertEquals(1, counter.updateStampCounter.get());
    }

    @Test
    void testOneMemberJoinsCluster() {
        // configure member with high partition count
        Config config = HazelcastTestSupport.smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "" + 20000);
        // setup interceptor service that will track partition state stamp updates on joining member
        ConfigAccessor.getServicesConfig(config)
                .addServiceConfig(
                        new ServiceConfig()
                                .setName("InjectInterceptorService")
                                .setEnabled(true)
                                .setClassName(InjectInterceptorService.class.getName())
                );
        HazelcastInstance[] members = factory.newInstances(config, 2);
        Accessors.getPartitionService(members[0]).firstArrangement();
        HazelcastTestSupport.waitClusterForSafeState(members[0]);

        // do not allow migrations to be triggered as soon as member joins - we only want to measure the
        // effect of applying the initial partition state, as provided from master to joining member
        members[0].getCluster().changeClusterState(ClusterState.NO_MIGRATION);
        HazelcastTestSupport.assertClusterStateEventually(ClusterState.NO_MIGRATION, members);
        // start a new member
        HazelcastInstance joiningMember = factory.newHazelcastInstance(config);
        HazelcastTestSupport.assertClusterSizeEventually(3, joiningMember);
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) Accessors.getPartitionService(joiningMember);
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        HazelcastTestSupport.assertTrueEventually(() -> assertTrue(partitionStateManager.isInitialized()));

        assertEquals(1, REPLICA_UPDATE_COUNTER.batchReplicaCounter.get());
        assertEquals(1, REPLICA_UPDATE_COUNTER.updateStampCounter.get());
    }

    static final class ReplicaUpdateCounter implements PartitionStateManager.ReplicaUpdateInterceptor {
        final AtomicInteger batchReplicaCounter = new AtomicInteger();
        final AtomicInteger updateStampCounter = new AtomicInteger();
        @Override
        public void onPartitionOwnersChanged() {
            batchReplicaCounter.incrementAndGet();
        }

        @Override
        public void onPartitionStampUpdate() {
            updateStampCounter.incrementAndGet();
        }

        void reset() {
            batchReplicaCounter.set(0);
            updateStampCounter.set(0);
        }
    }

    public static class InjectInterceptorService implements CoreService, PreJoinAwareService<InjectInterceptorOp> {
        @Override
        public InjectInterceptorOp getPreJoinOperation() {
            return new InjectInterceptorOp();
        }
    }

    public static class InjectInterceptorOp extends Operation implements AllowedDuringPassiveState {
        // replica update counter is only used in a single test, so even though static it doesn't matter that we don't reset it
        static final ReplicaUpdateCounter REPLICA_UPDATE_COUNTER = new ReplicaUpdateCounter();

        @Override
        public void run() {
            // only act on the joining member
            if (getNodeEngine().getThisAddress().getPort() == 5703) {
                InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getNodeEngine().getPartitionService();
                PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
                // replace the default no-op implementation with the counter implementation
                partitionStateManager.setReplicaUpdateInterceptor(REPLICA_UPDATE_COUNTER);
            }
        }
    }
}
