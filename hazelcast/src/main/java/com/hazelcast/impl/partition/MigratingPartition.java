/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.partition;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MigratingPartition implements DataSerializable {
    protected int partitionId;
    protected Address from;
    protected Address to;
    protected int replicaIndex;

    private transient final long creationTime = System.currentTimeMillis();

    public MigratingPartition() {
    }

    public MigratingPartition(int partitionId, int replicaIndex, Address from, Address to) {
        this.partitionId = partitionId;
        this.from = from;
        this.to = to;
        this.replicaIndex = replicaIndex;
    }

    public Address getFromAddress() {
        return from;
    }

    public Address getToAddress() {
        return to;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(replicaIndex);
        boolean hasFrom = from != null;
        out.writeBoolean(hasFrom);
        if (hasFrom) {
            from.writeData(out);
        }
        to.writeData(out);
    }

    public void readData(DataInput in) throws IOException {
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        boolean hasFrom = in.readBoolean();
        if (hasFrom) {
            from = new Address();
            from.readData(in);
        }
        to = new Address();
        to.readData(in);
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("MigratingPartition");
        sb.append("{partitionId=").append(partitionId);
        sb.append(", from=").append(from);
        sb.append(", to=").append(to);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof MigratingPartition)) return false;
        final MigratingPartition that = (MigratingPartition) o;
        if (partitionId != that.partitionId) return false;
        if (replicaIndex != that.replicaIndex) return false;
        if (from != null ? !from.equals(that.from) : that.from != null) return false;
        if (to != null ? !to.equals(that.to) : that.to != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + replicaIndex;
        return result;
    }
}
