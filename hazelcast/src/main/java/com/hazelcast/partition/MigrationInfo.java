/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class MigrationInfo implements DataSerializable {

    private int partitionId;
    private Address from;
    private Address to;
    private int replicaIndex;
    private MigrationType migrationType;
    private int copyBackReplicaIndex = -1;
    private Address master;
    private String masterUuid;

    private transient final AtomicBoolean processing = new AtomicBoolean(false);
    private transient volatile boolean valid = true;

    public MigrationInfo() {
    }

    public MigrationInfo(int partitionId, int replicaIndex, MigrationType migrationType,
                         Address from, Address to) {
        this.partitionId = partitionId;
        this.from = from;
        this.to = to;
        this.replicaIndex = replicaIndex;
        this.migrationType = migrationType;
    }

    public Address getFromAddress() {
        return from;
    }

    void setFromAddress(Address fromAddress) {
        this.from = fromAddress;
    }

    public Address getToAddress() {
        return to;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getCopyBackReplicaIndex() {
        return copyBackReplicaIndex;
    }

    void setCopyBackReplicaIndex(int copyBackReplicaIndex) {
        this.copyBackReplicaIndex = copyBackReplicaIndex;
    }

    public MigrationType getMigrationType() {
        return migrationType;
    }

    void setMigrationType(MigrationType migrationType) {
        this.migrationType = migrationType;
    }

    void setMasterUuid(String uuid) {
        masterUuid = uuid;
    }

    String getMasterUuid() {
        return masterUuid;
    }

    Address getMaster() {
        return master;
    }

    void setMaster(Address master) {
        this.master = master;
    }

    boolean startProcessing() {
        return processing.compareAndSet(false, true);
    }

    boolean isProcessing() {
        return processing.get();
    }

    void doneProcessing() {
        processing.set(false);
    }

    boolean isValid() {
        return valid;
    }

    void invalidate() {
        valid = false;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(replicaIndex);
        out.writeInt(copyBackReplicaIndex);
        MigrationType.writeTo(migrationType, out);
        boolean hasFrom = from != null;
        out.writeBoolean(hasFrom);
        if (hasFrom) {
            from.writeData(out);
        }
        to.writeData(out);
        master.writeData(out);
        out.writeUTF(masterUuid);
    }

    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        copyBackReplicaIndex = in.readInt();
        migrationType = MigrationType.readFrom(in);
        boolean hasFrom = in.readBoolean();
        if (hasFrom) {
            from = new Address();
            from.readData(in);
        }
        to = new Address();
        to.readData(in);
        master = new Address();
        master.readData(in);
        masterUuid = in.readUTF();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final MigrationInfo that = (MigrationInfo) o;

        if (copyBackReplicaIndex != that.copyBackReplicaIndex) return false;
        if (partitionId != that.partitionId) return false;
        if (replicaIndex != that.replicaIndex) return false;
        if (migrationType != that.migrationType) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + replicaIndex;
        result = 31 * result + copyBackReplicaIndex;
        result = 31 * result + (migrationType != null ? migrationType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MigrationInfo");
        sb.append("{ partitionId=").append(partitionId);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append(", copyBackReplicaIndex=").append(copyBackReplicaIndex);
        sb.append(", migrationType=").append(migrationType);
        sb.append(", source=").append(from);
        sb.append(", destination=").append(to);
        sb.append(", master=").append(master);
        sb.append(", processing=").append(processing.get());
        sb.append('}');
        return sb.toString();
    }
}
