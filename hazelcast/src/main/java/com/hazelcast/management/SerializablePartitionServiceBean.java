package com.hazelcast.management;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.InternalPartitionService;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A Serializable DTO for {@link com.hazelcast.jmx.PartitionServiceMBean}.
 */
public class SerializablePartitionServiceBean implements DataSerializable {

    private int partitionCount;
    private int activePartitionCount;

    public SerializablePartitionServiceBean() {
    }

    public SerializablePartitionServiceBean(InternalPartitionService partitionService,
                                            HazelcastInstanceImpl hazelcastInstance) {
        InetSocketAddress address = hazelcastInstance.getCluster().getLocalMember().getSocketAddress();
        this.partitionCount = partitionService.getPartitionCount();
        this.activePartitionCount = partitionService.getMemberPartitions(new Address(address)).size();
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    public int getActivePartitionCount() {
        return activePartitionCount;
    }

    public void setActivePartitionCount(int activePartitionCount) {
        this.activePartitionCount = activePartitionCount;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionCount);
        out.writeInt(activePartitionCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionCount = in.readInt();
        activePartitionCount = in.readInt();
    }
}
