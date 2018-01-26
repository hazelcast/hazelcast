/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.partition;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializableByConvention;

import java.io.IOException;

import static com.hazelcast.nio.serialization.SerializableByConvention.Reason.PUBLIC_API;

/**
 * Internal event that is dispatched to {@link com.hazelcast.spi.PartitionAwareService#onPartitionLost}
 * <p>
 * It contains the partition ID, number of replicas that is lost and the address of node that detects the partition lost.
 */
@SerializableByConvention(PUBLIC_API)
public class IPartitionLostEvent
        implements DataSerializable {

    private int partitionId;

    private int lostReplicaIndex;

    private Address eventSource;

    public IPartitionLostEvent() {
    }

    public IPartitionLostEvent(int partitionId, int lostReplicaIndex, Address eventSource) {
        this.partitionId = partitionId;
        this.lostReplicaIndex = lostReplicaIndex;
        this.eventSource = eventSource;
    }

    /**
     * The partition ID that is lost.
     *
     * @return the partition ID that is lost
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * 0-based replica index that is lost for the partition.
     * <p>
     * For instance, 0 means only the owner of the partition is lost, 1 means both the owner and first backup are lost.
     *
     * @return 0-based replica index that is lost for the partition
     */
    public int getLostReplicaIndex() {
        return lostReplicaIndex;
    }

    /**
     * The address of the node that detects the partition lost.
     *
     * @return the address of the node that detects the partition lost
     */
    public Address getEventSource() {
        return eventSource;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(lostReplicaIndex);
        eventSource.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
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
