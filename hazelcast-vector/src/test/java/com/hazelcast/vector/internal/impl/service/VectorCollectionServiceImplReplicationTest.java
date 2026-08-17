/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.service;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.vector.internal.impl.VectorCollectionService;
import com.hazelcast.vector.internal.impl.ops.ReplicationOperation;
import com.hazelcast.vector.internal.impl.storage.VectorCollectionStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;

import static com.hazelcast.vector.internal.impl.VectorTestUtils.warmupCollection;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorCollectionServiceImplReplicationTest extends HazelcastTestSupport {

    TestHazelcastInstanceFactory factory;
    private HazelcastInstance member;
    private VectorCollectionServiceImpl vectorCollectionService;
    private int partitionCount;

    @Before
    public void setup() {
        factory = new TestHazelcastInstanceFactory();
        member = factory.newHazelcastInstance(smallInstanceConfigWithoutJetAndMetrics());
        vectorCollectionService = Accessors.getNodeEngineImpl(member).getService(VectorCollectionService.SERVICE_NAME);
        partitionCount = member.getPartitionService().getPartitions().size();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testGetAllServiceNamespaces_whenProxiesExist() {
        ServiceNamespace[] expectedNamespaces = new ServiceNamespace[10];
        for (int i = 0; i < 10; i++) {
            var collectionName = randomName();
            warmupCollection(member, collectionName, 128, Metric.EUCLIDEAN);
            expectedNamespaces[i] = new DistributedObjectNamespace(VectorCollectionService.SERVICE_NAME, collectionName);
        }
        for (int i = 0; i < partitionCount; i++) {
            assertThat(vectorCollectionService.getAllServiceNamespaces(newReplicationEvent(i, 0)))
                    .as("validate service namespaces for partitionId %d", i)
                    .containsExactlyInAnyOrder(expectedNamespaces);
        }
    }

    @Test
    public void testGetAllServiceNamespaces_whenNoProxiesExist() {
        assertThat(vectorCollectionService.getAllServiceNamespaces(newReplicationEvent(0, 0)))
                .isEmpty();
    }

    @Test
    public void testPrepareReplicationOp_whenProxiesExist() {
        String[] collectionNames = new String[10];
        for (int i = 0; i < 10; i++) {
            collectionNames[i] = randomName();
            warmupCollection(member, collectionNames[i], 128, Metric.EUCLIDEAN);
        }
        for (int i = 0; i < partitionCount; i++) {
            ReplicationOperation op = (ReplicationOperation) vectorCollectionService.prepareReplicationOperation(
                    newReplicationEvent(i, 0));
            Map<String, VectorCollectionStorage> toBeMigrated = op.getReplicationStateHolder().getStorageMap();
            assertThat(toBeMigrated.keySet()).as("all namespaces should be added to replication operation")
                    .containsExactlyInAnyOrder(collectionNames);
        }
    }

    @Test
    public void testPrepareReplicationOp_whenSpecificNamespaceRequested() {
        String[] collectionNames = new String[10];
        for (int i = 0; i < 10; i++) {
            collectionNames[i] = randomName();
            warmupCollection(member, collectionNames[i], 128, Metric.EUCLIDEAN);
        }
        for (int i = 0; i < partitionCount; i++) {
            ReplicationOperation op = (ReplicationOperation) vectorCollectionService.prepareReplicationOperation(
                    newReplicationEvent(i, 0),
                    Collections.singleton(
                            new DistributedObjectNamespace(VectorCollectionService.SERVICE_NAME, collectionNames[0])));
            Map<String, VectorCollectionStorage> toBeMigrated = op.getReplicationStateHolder().getStorageMap();
            assertThat(toBeMigrated.keySet()).as("one namespace should be added to replication operation")
                    .containsExactly(collectionNames[0]);
        }
    }

    @Test
    public void testPrepareReplicationOp_whenNoProxiesExist() {
        assertThat(vectorCollectionService.prepareReplicationOperation(newReplicationEvent(0, 0)))
                .isNull();
    }

    PartitionReplicationEvent newReplicationEvent(int partitionId, int replicaIndex) {
        try {
            return new PartitionReplicationEvent(new Address("127.0.0.1", 5701), partitionId, replicaIndex);
        } catch (UnknownHostException e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }
}
