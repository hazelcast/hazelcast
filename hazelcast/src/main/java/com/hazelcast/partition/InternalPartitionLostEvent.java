package com.hazelcast.partition;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.annotation.PrivateApi;

import java.io.IOException;

/**
 * Internal event that is dispatched to @see com.hazelcast.spi.PartitionAwareService#onPartitionLost()
 * <p/>
 * It contains the partition id, number of replicas that is lost and the address of node that detects the partition lost
 */
@PrivateApi
public class InternalPartitionLostEvent
        implements DataSerializable {

    private int partitionId;

    private int lostReplicaIndex;

    private Address eventSource;

    public InternalPartitionLostEvent() {
    }

    public InternalPartitionLostEvent(int partitionId, int lostReplicaIndex, Address eventSource) {
        this.partitionId = partitionId;
        this.lostReplicaIndex = lostReplicaIndex;
        this.eventSource = eventSource;
    }

    /**
     * The partition id that is lost
     *
     * @return the partition id that is lost
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * 0-based replica index that is lost for the partition
     * For instance, 0 means only the owner of the partition is lost, 1 means both the owner and first backup are lost
     *
     * @return 0-based replica index that is lost for the partition
     */
    public int getLostReplicaIndex() {
        return lostReplicaIndex;
    }

    /**
     * The address of the node that detects the partition lost
     *
     * @return the address of the node that detects the partition lost
     */
    public Address getEventSource() {
        return eventSource;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeInt(partitionId);
        out.writeInt(lostReplicaIndex);
        eventSource.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        this.partitionId = in.readInt();
        this.lostReplicaIndex = in.readInt();
        this.eventSource = new Address();
        this.eventSource.readData(in);
    }

    @Override
    public String toString() {
        return getClass().getName() + "{partitionId=" + partitionId + ", lostReplicaIndex=" + lostReplicaIndex + ", eventSource="
                + eventSource + '}';
    }

}
