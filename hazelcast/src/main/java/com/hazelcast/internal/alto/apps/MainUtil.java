package com.hazelcast.internal.alto.apps;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.net.SocketAddress;

public class MainUtil {


    public static int findPartition(HazelcastInstance node) {
        Integer x = 0;
        SocketAddress localAddress = node.getLocalEndpoint().getSocketAddress();
        PartitionService partitionService = node.getPartitionService();
        for (; ; ) {
            Partition partition = partitionService.getPartition(x);
            if(partition == null||partition.getOwner()==null){
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                continue;
            }
            if (localAddress.equals(partition.getOwner().getSocketAddress())) {
                return partition.getPartitionId();
            }
            x++;
        }
    }

}
