/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;

/**
 * An immutable/readonly view of partition table.
 * View consists of partition replica assignments and global partition state version.
 * <p>
 * {@link #getAddresses(int)} returns clone of internal addresses array.
 */
public class PartitionTableView {

    private final Address[][] addresses;
    private final int version;

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public PartitionTableView(Address[][] addresses, int version) {
        this.addresses = addresses;
        this.version = version;
    }

    public PartitionTableView(InternalPartition[] partitions, int version) {
        Address[][] a = new Address[partitions.length][MAX_REPLICA_COUNT];
        for (InternalPartition partition : partitions) {
            int partitionId = partition.getPartitionId();
            for (int replica = 0; replica < MAX_REPLICA_COUNT; replica++) {
                a[partitionId][replica] = partition.getReplicaAddress(replica);
            }
        }

        this.addresses = a;
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public Address getAddress(int partitionId, int replicaIndex) {
        return addresses[partitionId][replicaIndex];
    }

    public int getLength() {
        return addresses.length;
    }

    public Address[] getAddresses(int partitionId) {
        Address[] a = addresses[partitionId];
        return Arrays.copyOf(a, a.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionTableView that = (PartitionTableView) o;
        return version == that.version && Arrays.deepEquals(addresses, that.addresses);
    }

    @Override
    public int hashCode() {
        int result = Arrays.deepHashCode(addresses);
        result = 31 * result + version;
        return result;
    }

    @Override
    public String toString() {
        return "PartitionTable{" + "addresses=" + Arrays.deepToString(addresses) + ", version=" + version + '}';
    }

    public static void writeData(PartitionTableView table, ObjectDataOutput out) throws IOException {
        Address[][] addresses = table.addresses;
        out.writeInt(addresses.length);
        for (Address[] a : addresses) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                Address address = a[j];
                boolean addressExists = address != null;
                out.writeBoolean(addressExists);
                if (addressExists) {
                    address.writeData(out);
                }
            }
        }

        out.writeInt(table.version);
    }

    public static PartitionTableView readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        Address[][] addresses = new Address[len][MAX_REPLICA_COUNT];
        for (int i = 0; i < len; i++) {
            for (int j = 0; j < MAX_REPLICA_COUNT; j++) {
                boolean exists = in.readBoolean();
                if (exists) {
                    Address address = new Address();
                    addresses[i][j] = address;
                    address.readData(in);
                }
            }
        }

        int version = in.readInt();
        return new PartitionTableView(addresses, version);
    }
}
