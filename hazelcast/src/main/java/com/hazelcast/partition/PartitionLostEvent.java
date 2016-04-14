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

package com.hazelcast.partition;

import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.internal.partition.InternalPartition;
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
     * If all replicas of a partition is lost, {@link InternalPartition#MAX_BACKUP_COUNT} is returned.
     *
     * @return the number of lost backups for the partition.
     * If all replicas of a partition is lost, {@link InternalPartition#MAX_BACKUP_COUNT} is returned.
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
        out.writeObject(eventSource);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        partitionId = in.readInt();
        lostBackupCount = in.readInt();
        eventSource = in.readObject();
    }

    @Override
    public String toString() {
        return getClass().getName() + "{partitionId=" + partitionId + ", lostBackupCount=" + lostBackupCount + ", eventSource="
                + eventSource + '}';
    }
}
