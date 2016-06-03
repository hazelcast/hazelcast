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

import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

public final class PartitionRuntimeState implements IdentifiedDataSerializable {

    // used for writing state
    private Map<Address, Integer> addressToIndexes;

    // used for reading state
    private Address[] addresses;

    private int[][] minimizedPartitionTable;
    private int version;
    private Collection<MigrationInfo> completedMigrations;
    // used to know ongoing migrations when master changed
    private MigrationInfo activeMigration;

    private Address endpoint;

    public PartitionRuntimeState() {
    }

    public PartitionRuntimeState(InternalPartition[] partitions, Collection<MigrationInfo> migrationInfos, int version) {
        this.version = version;
        completedMigrations = migrationInfos != null ? migrationInfos : Collections.<MigrationInfo>emptyList();
        addressToIndexes = createAddressToIndexMap(partitions);
        minimizedPartitionTable = createMinimizedPartitionTable(partitions);
    }

    private int[][] createMinimizedPartitionTable(InternalPartition[] partitions) {
        int[][] partitionTable = new int[partitions.length][MAX_REPLICA_COUNT];

        for (InternalPartition partition : partitions) {
            int[] indexes = partitionTable[partition.getPartitionId()];

            for (int replicaIndex = 0; replicaIndex < MAX_REPLICA_COUNT; replicaIndex++) {
                Address address = partition.getReplicaAddress(replicaIndex);
                if (address == null) {
                    indexes[replicaIndex] = -1;
                } else {
                    int index = addressToIndexes.get(address);
                    indexes[replicaIndex] = index;
                }
            }
        }
        return partitionTable;
    }

    private Map<Address, Integer> createAddressToIndexMap(InternalPartition[] partitions) {
        Map<Address, Integer> map = new HashMap<Address, Integer>();
        int addressIndex = 0;
        for (InternalPartition partition : partitions) {
            for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
                Address address = partition.getReplicaAddress(i);
                if (address == null) {
                    continue;
                }
                if (map.containsKey(address)) {
                    continue;
                }
                map.put(address, addressIndex++);
            }
        }
        return map;
    }

    public Address[][] getPartitionTable() {
        if (addresses == null) {
            addresses = new Address[addressToIndexes.size()];
            for (Map.Entry<Address, Integer> entry : addressToIndexes.entrySet()) {
                addresses[entry.getValue()] = entry.getKey();
            }
        }

        int length = minimizedPartitionTable.length;
        Address[][] result = new Address[length][MAX_REPLICA_COUNT];
        for (int partitionId = 0; partitionId < length; partitionId++) {
            Address[] replicas = result[partitionId];
            int[] addressIndexes = minimizedPartitionTable[partitionId];
            for (int replicaIndex = 0; replicaIndex < addressIndexes.length; replicaIndex++) {
                int index = addressIndexes[replicaIndex];
                if (index != -1) {
                    Address address = addresses[index];
                    assert address != null;
                    replicas[replicaIndex] = address;
                }
            }
        }
        return result;
    }

    public Address getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(final Address endpoint) {
        this.endpoint = endpoint;
    }

    public Collection<MigrationInfo> getCompletedMigrations() {
        return completedMigrations != null ? completedMigrations : Collections.<MigrationInfo>emptyList();
    }

    public MigrationInfo getActiveMigration() {
        return activeMigration;
    }

    public void setActiveMigration(MigrationInfo activeMigration) {
        this.activeMigration = activeMigration;
    }

    public void setCompletedMigrations(Collection<MigrationInfo> completedMigrations) {
        this.completedMigrations = completedMigrations;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        version = in.readInt();
        int memberCount = in.readInt();
        addresses = new Address[memberCount];
        for (int i = 0; i < memberCount; i++) {
            Address address = new Address();
            address.readData(in);
            int index = in.readInt();
            assert addresses[index] == null : "Duplicate address! Address: " + address + ", index: " + index
                    + ", addresses: " + Arrays.toString(addresses);
            addresses[index] = address;
        }

        int partitionCount = in.readInt();
        minimizedPartitionTable = new int[partitionCount][MAX_REPLICA_COUNT];
        for (int i = 0; i < partitionCount; i++) {
            int[] indexes = minimizedPartitionTable[i];
            for (int ix = 0; ix < MAX_REPLICA_COUNT; ix++) {
                indexes[ix] = in.readInt();
            }
        }

        if (in.readBoolean()) {
            activeMigration = new MigrationInfo();
            activeMigration.readData(in);
        }

        int k = in.readInt();
        if (k > 0) {
            completedMigrations = new ArrayList<MigrationInfo>(k);
            for (int i = 0; i < k; i++) {
                MigrationInfo migrationInfo = new MigrationInfo();
                migrationInfo.readData(in);
                completedMigrations.add(migrationInfo);
            }
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(version);
        if (addressToIndexes == null) {
            out.writeInt(addresses.length);
            for (int index = 0; index < addresses.length; index++) {
                Address address = addresses[index];
                address.writeData(out);
                out.writeInt(index);
            }
        } else {
            int memberCount = addressToIndexes.size();
            out.writeInt(memberCount);
            for (Map.Entry<Address, Integer> entry : addressToIndexes.entrySet()) {
                Address address = entry.getKey();
                address.writeData(out);
                int index = entry.getValue();
                out.writeInt(index);
            }
        }

        out.writeInt(minimizedPartitionTable.length);
        for (int[] indexes : minimizedPartitionTable) {
            for (int ix = 0; ix < MAX_REPLICA_COUNT; ix++) {
                out.writeInt(indexes[ix]);
            }
        }

        if (activeMigration != null) {
            out.writeBoolean(true);
            activeMigration.writeData(out);
        } else {
            out.writeBoolean(false);
        }

        if (completedMigrations != null) {
            int k = completedMigrations.size();
            out.writeInt(k);
            for (MigrationInfo migrationInfo : completedMigrations) {
                migrationInfo.writeData(out);
            }
        } else {
            out.writeInt(0);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionRuntimeState [" + version + "]{" + LINE_SEPARATOR);

        if (addressToIndexes == null) {
            //addressToIndexes is null after deserialization. let's use the array
            for (Address address : addresses) {
                sb.append(address).append(LINE_SEPARATOR);
            }
        } else {
            for (Address address : addressToIndexes.keySet()) {
                sb.append(address).append(LINE_SEPARATOR);
            }
        }
        sb.append(", completedMigrations=").append(completedMigrations);
        sb.append('}');
        return sb.toString();
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.PARTITION_STATE;
    }

}
