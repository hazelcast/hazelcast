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

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;

import java.io.IOException;

/**
 * An event fired when a partition migration starts, completes or fails.
 *
 * @see Partition
 * @see PartitionService
 * @see MigrationListener
 */
public class ReplicaMigrationEventImpl implements ReplicaMigrationEvent, IdentifiedDataSerializable {

    private MigrationState state;
    private int partitionId;
    private int replicaIndex;
    private Member source;
    private Member destination;
    private boolean success;
    private long elapsedTime;

    public ReplicaMigrationEventImpl() {
    }

    public ReplicaMigrationEventImpl(MigrationState state, int partitionId, int replicaIndex, Member source, Member destination,
            boolean success, long elapsedTime) {
        this.state = state;
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.source = source;
        this.destination = destination;
        this.success = success;
        this.elapsedTime = elapsedTime;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public int getReplicaIndex() {
        return replicaIndex;
    }

    @Override
    public Member getSource() {
        return source;
    }

    @Override
    public Member getDestination() {
        return destination;
    }

    @Override
    public MigrationState getMigrationState() {
        return state;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }

    @Override
    public long getElapsedTime() {
        return elapsedTime;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(state);
        out.writeInt(partitionId);
        out.writeInt(replicaIndex);
        out.writeObject(source);
        out.writeObject(destination);
        out.writeBoolean(success);
        out.writeLong(elapsedTime);
    }

    public void readData(ObjectDataInput in) throws IOException {
        state = in.readObject();
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        source = in.readObject();
        destination = in.readObject();
        success = in.readBoolean();
        elapsedTime = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.REPLICA_MIGRATION_EVENT;
    }

    @Override
    public String toString() {
        return "MigrationEvent{" + "state=" + state + ", partitionId=" + partitionId + ", replicaIndex=" + replicaIndex
                + ", source=" + source + ", destination=" + destination + ", success=" + success + ", elapsedTime=" + elapsedTime
                + "ms}";
    }
}
