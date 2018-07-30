/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.InternalPartitionImplConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class InternalPartitionImplConstructorTest {

    @Test
    public void testConstructor() throws Exception {
        Address address = new Address("172.16.16.1", 4223);
        Address[] addresses = new Address[]{new Address("127.0.0.1", 2342)};
        InternalPartition partition = new InternalPartitionImpl(42, null, address, addresses);

        InternalPartitionImplConstructor constructor = new InternalPartitionImplConstructor(InternalPartitionImpl.class);
        InternalPartition clonedPartition = (InternalPartition) constructor.createNew(partition);

        assertEquals(partition.getPartitionId(), clonedPartition.getPartitionId());
        assertEquals(partition.getOwnerOrNull(), clonedPartition.getOwnerOrNull());
        assertEquals(partition.getReplicaAddress(0), clonedPartition.getReplicaAddress(0));
        assertEquals(partition.getReplicaIndex(addresses[0]), clonedPartition.getReplicaIndex(addresses[0]));
    }
}
