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
    private Address source;
    private Address destination;
    private Address master;
    private String masterUuid;

    private final AtomicBoolean processing = new AtomicBoolean(false);
    private volatile boolean valid = true;

    public MigrationInfo() {
    }

    public MigrationInfo(int partitionId, Address source, Address destination) {
        this.partitionId = partitionId;
        this.source = source;
        this.destination = destination;
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

    public boolean isValid() {
        return valid;
    }

    public void invalidate() {
        valid = false;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MigrationInfo that = (MigrationInfo) o;

        if (partitionId != that.partitionId) {
            return false;
        }
        if (destination != null ? !destination.equals(that.destination) : that.destination != null) {
            return false;
        }
        if (masterUuid != null ? !masterUuid.equals(that.masterUuid) : that.masterUuid != null) {
            return false;
        }
        if (source != null ? !source.equals(that.source) : that.source != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + (source != null ? source.hashCode() : 0);
        result = 31 * result + (destination != null ? destination.hashCode() : 0);
        result = 31 * result + (masterUuid != null ? masterUuid.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MigrationInfo");
        sb.append("{ partitionId=").append(partitionId);
        sb.append(", source=").append(source);
        sb.append(", destination=").append(destination);
        sb.append(", master=").append(master);
        sb.append(", valid=").append(valid);
        sb.append(", processing=").append(processing.get());
        sb.append('}');
        return sb.toString();
    }
}
