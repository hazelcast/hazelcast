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

package com.hazelcast.internal.partition;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.MigrationType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class MigrationInfo implements DataSerializable {

    public enum MigrationStatus {

        ACTIVE(0),
        INVALID(1),
        SUCCESS(2),
        FAILED(3);

        public static void writeTo(MigrationStatus type, DataOutput out) throws IOException {
            out.writeByte(type.code);
        }

        public static MigrationStatus readFrom(DataInput in) throws IOException {
            final byte code = in.readByte();
            switch (code) {
                case 0:
                    return ACTIVE;
                case 1:
                    return INVALID;
                case 2:
                    return SUCCESS;
                case 3:
                    return FAILED;
            }
            throw new IllegalArgumentException("Code: " + code);
        }

        MigrationStatus(int code) {
            this.code = code;
        }

        private final int code;

    }

    private int partitionId;
    private int replicaIndex;
    private Address source;
    private Address destination;
    private Address master;
    private String masterUuid;
    private int keepReplicaIndex = -1;
    private MigrationType type = MigrationType.MOVE;

    private final AtomicBoolean processing = new AtomicBoolean(false);
    private volatile MigrationStatus status;

    public MigrationInfo() {
    }

    public MigrationInfo(int partitionId, int replicaIndex, Address source, Address destination) {
        this(partitionId, replicaIndex, source, destination, MigrationType.MOVE, -1);
    }

    public MigrationInfo(int partitionId, int replicaIndex, Address source, Address destination, MigrationType type) {
        this(partitionId, replicaIndex, source, destination, type, -1);
    }

    public MigrationInfo(int partitionId, int replicaIndex, Address source, Address destination,
            MigrationType type, int keepReplicaIndex) {
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.source = source;
        this.destination = destination;
        this.type = type;
        this.keepReplicaIndex = keepReplicaIndex;
        this.status = MigrationStatus.ACTIVE;
    }

    public Address getSource() {
        return source;
    }

    public Address getDestination() {
        return destination;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public int getKeepReplicaIndex() {
        return keepReplicaIndex;
    }

    public void setKeepReplicaIndex(int keepReplicaIndex) {
        this.keepReplicaIndex = keepReplicaIndex;
    }

    public MigrationType getType() {
        return type;
    }

    public void setType(MigrationType type) {
        this.type = type;
    }

    public void setMasterUuid(String uuid) {
        masterUuid = uuid;
    }

    public String getMasterUuid() {
        return masterUuid;
    }

    public Address getMaster() {
        return master;
    }

    public void setMaster(Address master) {
        this.master = master;
    }

    public boolean startProcessing() {
        return processing.compareAndSet(false, true);
    }

    public boolean isProcessing() {
        return processing.get();
    }

    public void doneProcessing() {
        processing.set(false);
    }

    public MigrationStatus getStatus() {
        return status;
    }

    public void setStatus(MigrationStatus status) {
        this.status = status;
    }

    public boolean isValid() {
        return status != MigrationStatus.INVALID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeByte(replicaIndex);
        out.writeByte(keepReplicaIndex);
        MigrationType.writeTo(type, out);
        MigrationStatus.writeTo(status, out);

        boolean hasFrom = source != null;
        out.writeBoolean(hasFrom);
        if (hasFrom) {
            source.writeData(out);
        }
        destination.writeData(out);

        out.writeUTF(masterUuid);
        boolean b = master != null;
        out.writeBoolean(b);
        if (b) {
            master.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
        replicaIndex = in.readByte();
        keepReplicaIndex = in.readByte();
        type = MigrationType.readFrom(in);
        status = MigrationStatus.readFrom(in);

        boolean hasFrom = in.readBoolean();
        if (hasFrom) {
            source = new Address();
            source.readData(in);
        }
        destination = new Address();
        destination.readData(in);

        masterUuid = in.readUTF();
        if (in.readBoolean()) {
            master = new Address();
            master.readData(in);
        }
    }

    //CHECKSTYLE:OFF
    // This equals method is to complex for our rules due to many internal object type members
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MigrationInfo that = (MigrationInfo) o;

        if (partitionId != that.partitionId) return false;
        if (replicaIndex != that.replicaIndex) return false;
        if (keepReplicaIndex != that.keepReplicaIndex) return false;
        if (source != null ? !source.equals(that.source) : that.source != null) return false;
        if (destination != null ? !destination.equals(that.destination) : that.destination != null) return false;
        if (masterUuid != null ? !masterUuid.equals(that.masterUuid) : that.masterUuid != null) return false;
        return type == that.type;
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + replicaIndex;
        result = 31 * result + (source != null ? source.hashCode() : 0);
        result = 31 * result + (destination != null ? destination.hashCode() : 0);
        result = 31 * result + (masterUuid != null ? masterUuid.hashCode() : 0);
        result = 31 * result + keepReplicaIndex;
        result = 31 * result + type.hashCode();
        return result;
    }
    //CHECKSTYLE:ON

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MigrationInfo{");
        sb.append("partitionId=").append(partitionId);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append(", source=").append(source);
        sb.append(", destination=").append(destination);
        sb.append(", master=").append(master);
        sb.append(", masterUuid='").append(masterUuid).append('\'');
        sb.append(", keepReplicaIndex=").append(keepReplicaIndex);
        sb.append(", type=").append(type);
        sb.append(", processing=").append(processing);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }
}
