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

import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.partition.PartitionService;

import java.io.IOException;

/**
 * The event that is fired when a partition lost its owner and all backups.
 *
 * @see Partition
 * @see PartitionService
 * @see PartitionLostListener
 */
public class PartitionLostEventImpl implements PartitionLostEvent, IPartitionLostEvent, IdentifiedDataSerializable {

    private int partitionId;

    private int lostBackupCount;

    private Address eventSource;

    public PartitionLostEventImpl() {
    }

    public PartitionLostEventImpl(int partitionId, int lostBackupCount, Address eventSource) {
        this.partitionId = partitionId;
        this.lostBackupCount = lostBackupCount;
        this.eventSource = eventSource;
    }

    /**
     * Returns the lost partition ID.
     *
     * @return the lost partition ID.
     */
    @Override
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Returns the number of lost backups for the partition. 0: the owner, 1: first backup, 2: second backup...
     * If all replicas of a partition are lost, {@link InternalPartition#MAX_BACKUP_COUNT} is returned.
     */
    @Override
    public int getLostBackupCount() {
        return lostBackupCount;
    }

    /**
     * Returns true if all replicas of a partition are lost
     */
    @Override
    public boolean allReplicasInPartitionLost() {
        return getLostBackupCount() == InternalPartition.MAX_BACKUP_COUNT;
    }

    @Override
    public int getLostReplicaIndex() {
        return lostBackupCount;
    }

    /**
     * Returns the address of the node that dispatches the event
     *
     * @return the address of the node that dispatches the event
     */
    @Override
    public Address getEventSource() {
        return eventSource;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(lostBackupCount);
        out.writeObject(eventSource);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
        lostBackupCount = in.readInt();
        eventSource = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.PARTITION_LOST_EVENT;
    }

    @Override
    public String toString() {
        return getClass().getName() + "{partitionId=" + partitionId + ", lostBackupCount=" + lostBackupCount + ", eventSource="
                + eventSource + '}';
    }
}
