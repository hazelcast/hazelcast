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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
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

import java.util.Collections;
import java.util.Set;

import static com.hazelcast.internal.partition.NonFragmentedServiceNamespace.INSTANCE;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionReplicaManagerTest extends HazelcastTestSupport {

    private static final int PARTITION_ID = 23;

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance hazelcastInstance;

    private PartitionReplicaManager manager;

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(1);
        hazelcastInstance = factory.newHazelcastInstance();

        Node node = getNode(hazelcastInstance);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hazelcastInstance);
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();

        manager = new PartitionReplicaManager(node, partitionService);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test(expected = AssertionError.class)
    public void testTriggerPartitionReplicaSync_whenReplicaIndexNegative_thenThrowException() {
        Set<ServiceNamespace> namespaces = Collections.<ServiceNamespace>singleton(INSTANCE);
        manager.triggerPartitionReplicaSync(PARTITION_ID, namespaces, -1);
    }

    @Test(expected = AssertionError.class)
    public void testTriggerPartitionReplicaSync_whenReplicaIndexTooLarge_thenThrowException() {
        Set<ServiceNamespace> namespaces = Collections.<ServiceNamespace>singleton(INSTANCE);
        manager.triggerPartitionReplicaSync(PARTITION_ID, namespaces, InternalPartition.MAX_REPLICA_COUNT + 1);
    }

    @Test
    public void testCheckSyncPartitionTarget_whenPartitionOwnerIsNull_thenReturnFalse() {
        assertNull(manager.checkAndGetPrimaryReplicaOwner(PARTITION_ID, 0));
    }

    @Test
    public void testCheckSyncPartitionTarget_whenNodeIsPartitionOwner_thenReturnFalse() {
        warmUpPartitions(hazelcastInstance);

        assertNull(manager.checkAndGetPrimaryReplicaOwner(PARTITION_ID, 0));
    }
}
