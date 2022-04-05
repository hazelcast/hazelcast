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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InternalPartitionTest extends HazelcastTestSupport {

    @Test
    public void test_isLocal_singleMember() {
        HazelcastInstance hz = createHazelcastInstance();
        InternalPartition partition = getNode(hz).getPartitionService().getPartition(0);
        assertTrue(partition.isLocal());
    }

    @Test
    public void test_isLocal_multiMember() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances();
        warmUpPartitions(cluster);

        Node node = getNode(cluster[0]);
        InternalPartitionService partitionService = node.getPartitionService();
        Address thisAddress = node.getThisAddress();
        for (int k = 0; k < partitionService.getPartitionCount(); k++) {
            InternalPartition internalPartition = partitionService.getPartition(k);
            if (thisAddress.equals(internalPartition.getOwnerOrNull())) {
                assertTrue("expecting local partition, but found a non local one", internalPartition.isLocal());
            } else {
                assertFalse("expecting non local partition, but found a local one", internalPartition.isLocal());
            }
        }
    }
}
