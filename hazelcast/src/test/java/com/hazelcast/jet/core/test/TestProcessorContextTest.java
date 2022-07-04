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

package com.hazelcast.jet.core.test;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestProcessorContextTest {

    @Test
    public void test() throws Exception {
        Address a0 = new Address("1.2.3.4", 0);

        TestProcessorContext c = new TestProcessorContext()
                .setPartitionAssignment(ImmutableMap.of(a0, new int[]{0, 3, 6}));

        HazelcastInstance mockHazelcastInstance = mock(HazelcastInstance.class);
        Cluster mockCluster = mock(Cluster.class);
        when(mockHazelcastInstance.getCluster()).thenReturn(mockCluster);
        Member mockMember = mock(Member.class);
        when(mockCluster.getLocalMember()).thenReturn(mockMember);
        when(mockMember.getAddress()).thenReturn(a0);

        c.setHazelcastInstance(mockHazelcastInstance);
        assertArrayEquals(new int[]{0, 3, 6}, c.memberPartitions());

        c.setLocalParallelism(2);
        c.setLocalProcessorIndex(0);
        assertArrayEquals(new int[]{0, 6}, c.processorPartitions());
        c.setLocalProcessorIndex(1);
        assertArrayEquals(new int[]{3}, c.processorPartitions());

        c.setLocalParallelism(3);
        c.setLocalProcessorIndex(2);
        assertArrayEquals(new int[]{6}, c.processorPartitions());

        c.setLocalParallelism(4);
        c.setLocalProcessorIndex(3);
        assertArrayEquals(new int[]{}, c.processorPartitions());
    }
}
