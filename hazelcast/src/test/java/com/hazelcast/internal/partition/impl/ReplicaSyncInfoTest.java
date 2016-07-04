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

import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicaSyncInfoTest {

    private ReplicaSyncInfo replicaSyncInfo;
    private ReplicaSyncInfo replicaSyncInfoSameAttributes;

    private ReplicaSyncInfo replicaSyncInfoOtherTarget;
    private ReplicaSyncInfo replicaSyncInfoOtherPartitionId;
    private ReplicaSyncInfo replicaSyncInfoOtherReplicaIndex;

    @Before
    public void setUp() throws Exception {
        int partitionId = 23;
        int replicaIndex = 42;
        Address target = new Address("127.0.0.1", 5701);
        Address otherTarget = new Address("127.0.0.1", 5702);

        replicaSyncInfo = new ReplicaSyncInfo(partitionId, replicaIndex, target);
        replicaSyncInfoSameAttributes = new ReplicaSyncInfo(partitionId, replicaIndex, target);

        replicaSyncInfoOtherTarget = new ReplicaSyncInfo(partitionId, replicaIndex, otherTarget);
        replicaSyncInfoOtherPartitionId = new ReplicaSyncInfo(24, replicaIndex, target);
        replicaSyncInfoOtherReplicaIndex = new ReplicaSyncInfo(partitionId, 43, target);
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

        assertNotEquals(replicaSyncInfo.hashCode(), replicaSyncInfoOtherPartitionId.hashCode());
        assertNotEquals(replicaSyncInfo.hashCode(), replicaSyncInfoOtherReplicaIndex.hashCode());
    }

    @Test
    public void testToString() {
        assertNotNull(replicaSyncInfo.toString());
    }
}
