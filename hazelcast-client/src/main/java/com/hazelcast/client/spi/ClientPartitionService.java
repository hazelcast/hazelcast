package com.hazelcast.client.spi;

import com.hazelcast.core.Partition;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;

/**
 * @mdogan 5/16/13
 */
public interface ClientPartitionService {

    Address getPartitionOwner(int partitionId);

    int getPartitionId(Data key);

    int getPartitionId(Object key);

    int getPartitionCount();

    Partition getPartition(int partitionId);
}
