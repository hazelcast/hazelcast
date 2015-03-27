package com.hazelcast.partition;

import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * The event that is fired when a partition lost its owner and all backups.
 *
 * @see Partition
 * @see PartitionService
 * @see PartitionLostListener
 */
public class PartitionLostEvent
        implements DataSerializable, PartitionEvent {

    private int partitionId;

    private int lostBackupCount;

    private Address eventSource;

    public PartitionLostEvent() {
    }

    public PartitionLostEvent(int partitionId, int lostBackupCount, Address eventSource) {
        this.partitionId = partitionId;
        this.lostBackupCount = lostBackupCount;
        this.eventSource = eventSource;
    }

    /**
     * Returns the lost partition id.
     *
     * @return the lost partition id.
     */
    @Override
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Returns the number of lost backups for the partition. O: the owner, 1: first backup, 2: second backup ...
     *
     * @return the number of lost backups for the partition
     */
    public int getLostBackupCount() {
        return lostBackupCount;
    }

    /**
     * Returns the address of the node that dispatches the event
     *
     * @return the address of the node that dispatches the event
     */
    public Address getEventSource() {
        return eventSource;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeInt(partitionId);
        out.writeInt(lostBackupCount);
        eventSource.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        partitionId = in.readInt();
        lostBackupCount = in.readInt();
        eventSource = new Address();
        eventSource.readData(in);
    }

    @Override
    public String toString() {
        return getClass().getName() + "{partitionId=" + partitionId + ", lostBackupCount=" + lostBackupCount + ", eventSource="
                + eventSource + '}';
    }
}
