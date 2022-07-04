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
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

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

public final class PartitionRuntimeState implements IdentifiedDataSerializable, Versioned {

    private PartitionReplica[] allReplicas;
    // Partition table encoded as an int matrix.
    // - 1st dimension index denotes the partitionId [0-partitionCount]
    // - 2nd dimension index denotes the replicaIndex [0-6]
    // - value at a point denotes the index of the partition replica in "allReplicas" array,
    // or -1 if the partition replica is not assigned.
    private int[][] encodedPartitionTable;
    private int[] versions;
    private long stamp;
    private Collection<MigrationInfo> completedMigrations;
    // used to know ongoing migrations when master changed
    private Collection<MigrationInfo> activeMigrations;

    /** The sender of the operation which changes the partition table, should be the master node */
    private transient Address master;

    public PartitionRuntimeState() {
    }

    public PartitionRuntimeState(InternalPartition[] partitions, Collection<MigrationInfo> completedMigrations, long stamp) {
        this.stamp = stamp;
        this.completedMigrations = completedMigrations != null ? completedMigrations : Collections.emptyList();
        Map<PartitionReplica, Integer> replicaToIndexes = createPartitionReplicaToIndexMap(partitions);
        allReplicas = toPartitionReplicaArray(replicaToIndexes);
        encodePartitionTable(partitions, replicaToIndexes);
    }

    private PartitionReplica[] toPartitionReplicaArray(Map<PartitionReplica, Integer> addressToIndexes) {
        PartitionReplica[] replicas = new PartitionReplica[addressToIndexes.size()];
        for (Map.Entry<PartitionReplica, Integer> entry : addressToIndexes.entrySet()) {
            replicas[entry.getValue()] = entry.getKey();
        }
        return replicas;
    }

    private void encodePartitionTable(InternalPartition[] partitions, Map<PartitionReplica, Integer> replicaToIndexes) {
        versions = new int[partitions.length];
        encodedPartitionTable = new int[partitions.length][MAX_REPLICA_COUNT];

        for (InternalPartition partition : partitions) {
            int[] indexes = encodedPartitionTable[partition.getPartitionId()];
            versions[partition.getPartitionId()] = partition.version();

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
    }

    private static Map<PartitionReplica, Integer> createPartitionReplicaToIndexMap(InternalPartition[] partitions) {
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

    public InternalPartition[] getPartitions() {
        int length = encodedPartitionTable.length;
        InternalPartition[] result = new InternalPartition[length];
        for (int partitionId = 0; partitionId < length; partitionId++) {
            int[] addressIndexes = encodedPartitionTable[partitionId];
            PartitionReplica[] replicas = new PartitionReplica[MAX_REPLICA_COUNT];
            for (int replicaIndex = 0; replicaIndex < addressIndexes.length; replicaIndex++) {
                int index = addressIndexes[replicaIndex];
                if (index != -1) {
                    PartitionReplica replica = allReplicas[index];
                    assert replica != null;
                    replicas[replicaIndex] = replica;
                }
            }
            result[partitionId] = new InternalPartitionImpl(partitionId, null, replicas, versions[partitionId], null);
        }
        return result;
    }

    public Address getMaster() {
        return master;
    }

    public void setMaster(Address master) {
        this.master = master;
    }

    public Collection<MigrationInfo> getCompletedMigrations() {
        return completedMigrations != null ? completedMigrations : Collections.emptyList();
    }

    public Collection<MigrationInfo> getActiveMigrations() {
        return activeMigrations;
    }

    public void setActiveMigrations(Collection<MigrationInfo> activeMigrations) {
        this.activeMigrations = activeMigrations;
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        stamp = in.readLong();

        int memberCount = in.readInt();
        allReplicas = new PartitionReplica[memberCount];
        for (int i = 0; i < memberCount; i++) {
            PartitionReplica replica = in.readObject();
            int index = in.readInt();
            assert allReplicas[index] == null : "Duplicate replica! Member: " + replica + ", index: " + index
                    + ", addresses: " + Arrays.toString(allReplicas);
            allReplicas[index] = replica;
        }

        int partitionCount = in.readInt();
        encodedPartitionTable = new int[partitionCount][MAX_REPLICA_COUNT];
        versions = new int[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            int[] indexes = encodedPartitionTable[i];
            for (int ix = 0; ix < MAX_REPLICA_COUNT; ix++) {
                indexes[ix] = in.readInt();
            }
        }

        for (int i = 0; i < partitionCount; i++) {
            versions[i] = in.readInt();
        }
        activeMigrations = readNullableCollection(in);
        completedMigrations = readNullableCollection(in);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(stamp);

        out.writeInt(allReplicas.length);
        for (int index = 0; index < allReplicas.length; index++) {
            PartitionReplica replica = allReplicas[index];
            out.writeObject(replica);
            out.writeInt(index);
        }

        out.writeInt(encodedPartitionTable.length);
        for (int[] indexes : encodedPartitionTable) {
            for (int ix = 0; ix < MAX_REPLICA_COUNT; ix++) {
                out.writeInt(indexes[ix]);
            }
        }

        for (int v : versions) {
            out.writeInt(v);
        }
        writeNullableCollection(activeMigrations, out);
        writeNullableCollection(completedMigrations, out);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionRuntimeState [" + stamp + "]{" + LINE_SEPARATOR);
        for (PartitionReplica replica : allReplicas) {
            sb.append(replica).append(LINE_SEPARATOR);
        }
        sb.append(", completedMigrations=").append(completedMigrations);
        sb.append('}');
        return sb.toString();
    }

    public long getStamp() {
        return stamp;
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
