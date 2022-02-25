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

import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicaSyncInfoTest {

    private ReplicaFragmentSyncInfo replicaSyncInfo;
    private ReplicaFragmentSyncInfo replicaSyncInfoSameAttributes;

    private ReplicaFragmentSyncInfo replicaSyncInfoOtherTarget;
    private ReplicaFragmentSyncInfo replicaSyncInfoOtherPartitionId;
    private ReplicaFragmentSyncInfo replicaSyncInfoOtherReplicaIndex;

    @Before
    public void setUp() throws Exception {
        int partitionId = 23;
        int replicaIndex = 42;
        PartitionReplica target = new PartitionReplica(new Address("127.0.0.1", 5701), new UUID(57, 1));
        PartitionReplica otherTarget = new PartitionReplica(new Address("127.0.0.1", 5702), new UUID(57, 2));

        replicaSyncInfo = new ReplicaFragmentSyncInfo(partitionId, NonFragmentedServiceNamespace.INSTANCE, replicaIndex, target);
        replicaSyncInfoSameAttributes
                = new ReplicaFragmentSyncInfo(partitionId, NonFragmentedServiceNamespace.INSTANCE, replicaIndex, target);

        replicaSyncInfoOtherTarget
                = new ReplicaFragmentSyncInfo(partitionId, NonFragmentedServiceNamespace.INSTANCE, replicaIndex, otherTarget);
        replicaSyncInfoOtherPartitionId
                = new ReplicaFragmentSyncInfo(24, NonFragmentedServiceNamespace.INSTANCE, replicaIndex, target);
        replicaSyncInfoOtherReplicaIndex
                = new ReplicaFragmentSyncInfo(partitionId, NonFragmentedServiceNamespace.INSTANCE, 43, target);
    }

    @Test
    public void testEquals() {
        assertEquals(replicaSyncInfo, replicaSyncInfo);
        assertEquals(replicaSyncInfo, replicaSyncInfoSameAttributes);
        assertEquals(replicaSyncInfo, replicaSyncInfoOtherTarget);

        assertNotEquals(replicaSyncInfo, null);
        assertNotEquals(replicaSyncInfo, new Object());

        assertNotEquals(replicaSyncInfo, replicaSyncInfoOtherPartitionId);
        assertNotEquals(replicaSyncInfo, replicaSyncInfoOtherReplicaIndex);
    }

    @Test
    public void testHashCode() {
        assertEquals(replicaSyncInfo.hashCode(), replicaSyncInfo.hashCode());
        assertEquals(replicaSyncInfo.hashCode(), replicaSyncInfoSameAttributes.hashCode());
        assertEquals(replicaSyncInfo.hashCode(), replicaSyncInfoOtherTarget.hashCode());

        assumeDifferentHashCodes();
        assertNotEquals(replicaSyncInfo.hashCode(), replicaSyncInfoOtherPartitionId.hashCode());
        assertNotEquals(replicaSyncInfo.hashCode(), replicaSyncInfoOtherReplicaIndex.hashCode());
    }

    @Test
    public void testToString() {
        assertNotNull(replicaSyncInfo.toString());
    }
}
