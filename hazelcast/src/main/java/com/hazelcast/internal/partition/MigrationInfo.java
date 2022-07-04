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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class MigrationInfo implements IdentifiedDataSerializable {

    public enum MigrationStatus {

        ACTIVE(0),
        SUCCESS(2),
        FAILED(3);

        private final int code;

        MigrationStatus(int code) {
            this.code = code;
        }

        public static void writeTo(MigrationStatus type, DataOutput out) throws IOException {
            out.writeByte(type.code);
        }

        public static MigrationStatus readFrom(DataInput in) throws IOException {
            final byte code = in.readByte();
            switch (code) {
                case 0:
                    return ACTIVE;
                case 2:
                    return SUCCESS;
                case 3:
                    return FAILED;
                default:
                    throw new IllegalArgumentException("Code: " + code);
            }
        }

    }

    private UUID uuid;
    private int partitionId;

    private PartitionReplica source;
    private PartitionReplica destination;
    private Address master;

    private int sourceCurrentReplicaIndex;
    private int sourceNewReplicaIndex;
    private int destinationCurrentReplicaIndex;
    private int destinationNewReplicaIndex;
    private int initialPartitionVersion = -1;
    private int partitionVersionIncrement;

    private volatile MigrationStatus status;

    public MigrationInfo() {
    }

    public MigrationInfo(int partitionId, PartitionReplica source, PartitionReplica destination,
            int sourceCurrentReplicaIndex, int sourceNewReplicaIndex,
            int destinationCurrentReplicaIndex, int destinationNewReplicaIndex) {
        this.uuid = UuidUtil.newUnsecureUUID();
        this.partitionId = partitionId;
        this.source = source;
        this.destination = destination;
        this.sourceCurrentReplicaIndex = sourceCurrentReplicaIndex;
        this.sourceNewReplicaIndex = sourceNewReplicaIndex;
        this.destinationCurrentReplicaIndex = destinationCurrentReplicaIndex;
        this.destinationNewReplicaIndex = destinationNewReplicaIndex;
        this.status = MigrationStatus.ACTIVE;
    }

    public PartitionReplica getSource() {
        return source;
    }

    public Address getSourceAddress() {
        return source != null ? source.address() : null;
    }

    public PartitionReplica getDestination() {
        return destination;
    }

    public Address getDestinationAddress() {
        return destination != null ? destination.address() : null;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getSourceCurrentReplicaIndex() {
        return sourceCurrentReplicaIndex;
    }

    public int getSourceNewReplicaIndex() {
        return sourceNewReplicaIndex;
    }

    public int getDestinationCurrentReplicaIndex() {
        return destinationCurrentReplicaIndex;
    }

    public int getDestinationNewReplicaIndex() {
        return destinationNewReplicaIndex;
    }

    public Address getMaster() {
        return master;
    }

    public MigrationInfo setMaster(Address master) {
        this.master = master;
        return this;
    }

    public MigrationStatus getStatus() {
        return status;
    }

    public MigrationInfo setStatus(MigrationStatus status) {
        this.status = status;
        return this;
    }

    public int getInitialPartitionVersion() {
        return initialPartitionVersion;
    }

    public MigrationInfo setInitialPartitionVersion(int initialPartitionVersion) {
        assert initialPartitionVersion > 0;
        this.initialPartitionVersion = initialPartitionVersion;
        return this;
    }

    public int getPartitionVersionIncrement() {
        if (partitionVersionIncrement > 0) {
            return partitionVersionIncrement;
        }
        int inc = 1;
        if (sourceNewReplicaIndex > -1) {
            inc++;
        }
        if (destinationCurrentReplicaIndex > -1) {
            inc++;
        }
        return inc;
    }

    public MigrationInfo setPartitionVersionIncrement(int partitionVersionIncrement) {
        assert partitionVersionIncrement > 0;
        this.partitionVersionIncrement = partitionVersionIncrement;
        return this;
    }

    public int getFinalPartitionVersion() {
        if (initialPartitionVersion > 0) {
            return initialPartitionVersion + getPartitionVersionIncrement();
        }
        throw new IllegalStateException("Initial partition version is not set!");
    }

    public UUID getUid() {
        return uuid;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, uuid);
        out.writeInt(partitionId);
        out.writeByte(sourceCurrentReplicaIndex);
        out.writeByte(sourceNewReplicaIndex);
        out.writeByte(destinationCurrentReplicaIndex);
        out.writeByte(destinationNewReplicaIndex);
        MigrationStatus.writeTo(status, out);
        out.writeObject(source);
        out.writeObject(destination);
        out.writeObject(master);
        out.writeInt(initialPartitionVersion);
        out.writeInt(partitionVersionIncrement);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        uuid = UUIDSerializationUtil.readUUID(in);
        partitionId = in.readInt();
        sourceCurrentReplicaIndex = in.readByte();
        sourceNewReplicaIndex = in.readByte();
        destinationCurrentReplicaIndex = in.readByte();
        destinationNewReplicaIndex = in.readByte();
        status = MigrationStatus.readFrom(in);
        source = in.readObject();
        destination = in.readObject();
        master = in.readObject();
        initialPartitionVersion = in.readInt();
        partitionVersionIncrement = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MigrationInfo that = (MigrationInfo) o;

        return uuid.equals(that.uuid);

    }

    @Override
    public int hashCode() {
        return uuid.hashCode();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MigrationInfo{");
        sb.append("uuid=").append(uuid);
        sb.append(", partitionId=").append(partitionId);
        sb.append(", source=").append(source);
        sb.append(", sourceCurrentReplicaIndex=").append(sourceCurrentReplicaIndex);
        sb.append(", sourceNewReplicaIndex=").append(sourceNewReplicaIndex);
        sb.append(", destination=").append(destination);
        sb.append(", destinationCurrentReplicaIndex=").append(destinationCurrentReplicaIndex);
        sb.append(", destinationNewReplicaIndex=").append(destinationNewReplicaIndex);
        sb.append(", master=").append(master);
        sb.append(", initialPartitionVersion=").append(initialPartitionVersion);
        sb.append(", partitionVersionIncrement=").append(getPartitionVersionIncrement());
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MIGRATION_INFO;
    }
}
