/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableCollection;
import static com.hazelcast.internal.util.StringUtil.LINE_SEPARATOR;

public final class PartitionRuntimeState implements IdentifiedDataSerializable {

    private PartitionReplica[] replicas;
    private int[][] minimizedPartitionTable;
    private int version;
    private Collection<MigrationInfo> completedMigrations;
    // used to know ongoing migrations when master changed
    private MigrationInfo activeMigration;

    /** The sender of the operation which changes the partition table, should be the master node */
    private Address master;

    public PartitionRuntimeState() {
    }

    public PartitionRuntimeState(InternalPartition[] partitions, Collection<MigrationInfo> completedMigrations, int version) {
        this.version = version;
        this.completedMigrations = completedMigrations != null ? completedMigrations : Collections.emptyList();
        Map<PartitionReplica, Integer> replicaToIndexes = createPartitionReplicaToIndexMap(partitions);
        replicas = toPartitionReplicaArray(replicaToIndexes);
        minimizedPartitionTable = createMinimizedPartitionTable(partitions, replicaToIndexes);
    }

    private PartitionReplica[] toPartitionReplicaArray(Map<PartitionReplica, Integer> addressToIndexes) {
        PartitionReplica[] replicas = new PartitionReplica[addressToIndexes.size()];
        for (Map.Entry<PartitionReplica, Integer> entry : addressToIndexes.entrySet()) {
            replicas[entry.getValue()] = entry.getKey();
        }
        return replicas;
    }

    private int[][] createMinimizedPartitionTable(InternalPartition[] partitions,
            Map<PartitionReplica, Integer> replicaToIndexes) {
        int[][] partitionTable = new int[partitions.length][MAX_REPLICA_COUNT];
        for (InternalPartition partition : partitions) {
            int[] indexes = partitionTable[partition.getPartitionId()];

            for (int replicaIndex = 0; replicaIndex < MAX_REPLICA_COUNT; replicaIndex++) {
                PartitionReplica replica = partition.getReplica(replicaIndex);
                if (replica == null) {
                    indexes[replicaIndex] = -1;
                } else {
                    int index = replicaToIndexes.get(replica);
                    indexes[replicaIndex] = index;
                }
            }
        }
        return partitionTable;
    }

    private Map<PartitionReplica, Integer> createPartitionReplicaToIndexMap(InternalPartition[] partitions) {
        Map<PartitionReplica, Integer> map = new HashMap<>();
        int addressIndex = 0;
        for (InternalPartition partition : partitions) {
            for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
                PartitionReplica replica = partition.getReplica(i);
                if (replica == null) {
                    continue;
                }
                if (map.containsKey(replica)) {
                    continue;
                }
                map.put(replica, addressIndex++);
            }
        }
        return map;
    }

    public PartitionReplica[][] getPartitionTable() {
        int length = minimizedPartitionTable.length;
        PartitionReplica[][] result = new PartitionReplica[length][MAX_REPLICA_COUNT];
        for (int partitionId = 0; partitionId < length; partitionId++) {
            int[] addressIndexes = minimizedPartitionTable[partitionId];
            for (int replicaIndex = 0; replicaIndex < addressIndexes.length; replicaIndex++) {
                int index = addressIndexes[replicaIndex];
                if (index != -1) {
                    PartitionReplica replica = replicas[index];
                    assert replica != null;
                    result[partitionId][replicaIndex] = replica;
                }
            }
        }
        return result;
    }

    public Address getMaster() {
        return master;
    }

    public void setMaster(final Address master) {
        this.master = master;
    }

    public Collection<MigrationInfo> getCompletedMigrations() {
        return completedMigrations != null ? completedMigrations : Collections.emptyList();
    }

    public MigrationInfo getActiveMigration() {
        return activeMigration;
    }

    public void setActiveMigration(MigrationInfo activeMigration) {
        this.activeMigration = activeMigration;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        version = in.readInt();
        int memberCount = in.readInt();
        replicas = new PartitionReplica[memberCount];
        for (int i = 0; i < memberCount; i++) {
            PartitionReplica replica = in.readObject();
            int index = in.readInt();
            assert replicas[index] == null : "Duplicate replica! Member: " + replica + ", index: " + index
                    + ", addresses: " + Arrays.toString(replicas);
            replicas[index] = replica;
        }

        int partitionCount = in.readInt();
        minimizedPartitionTable = new int[partitionCount][MAX_REPLICA_COUNT];
        for (int i = 0; i < partitionCount; i++) {
            int[] indexes = minimizedPartitionTable[i];
            for (int ix = 0; ix < MAX_REPLICA_COUNT; ix++) {
                indexes[ix] = in.readInt();
            }
        }

        activeMigration = in.readObject();
        completedMigrations = readNullableCollection(in);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(version);
        out.writeInt(replicas.length);
        for (int index = 0; index < replicas.length; index++) {
            PartitionReplica replica = replicas[index];
            out.writeObject(replica);
            out.writeInt(index);
        }

        out.writeInt(minimizedPartitionTable.length);
        for (int[] indexes : minimizedPartitionTable) {
            for (int ix = 0; ix < MAX_REPLICA_COUNT; ix++) {
                out.writeInt(indexes[ix]);
            }
        }

        out.writeObject(activeMigration);
        writeNullableCollection(completedMigrations, out);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionRuntimeState [" + version + "]{" + LINE_SEPARATOR);
        for (PartitionReplica replica : replicas) {
            sb.append(replica).append(LINE_SEPARATOR);
        }
        sb.append(", completedMigrations=").append(completedMigrations);
        sb.append('}');
        return sb.toString();
    }

    public int getVersion() {
        return version;
    }

    @Override
    public int getFactoryId() {
        return PartitionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.PARTITION_RUNTIME_STATE;
    }

}
