package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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
