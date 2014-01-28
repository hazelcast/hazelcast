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

import com.hazelcast.cluster.MemberInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.*;

/**
 * @author mdogan 5/7/12
 */
public class PartitionRuntimeState implements DataSerializable {
    private final static long serialVersionUID = 1;

    protected ArrayList<MemberInfo> members = new ArrayList<MemberInfo>(100);

    private Collection<ShortPartitionInfo> partitionInfos = new LinkedList<ShortPartitionInfo>();
    private long masterTime = Clock.currentTimeMillis();
    private int version;
    private Collection<MigrationInfo> completedMigrations;

    private transient Address endpoint;

    public PartitionRuntimeState() {
    }

    public PartitionRuntimeState(final Collection<MemberInfo> memberInfos,
                                 final InternalPartition[] partitions,
                                 final Collection<MigrationInfo> migrationInfos,
                                 final long masterTime, int version) {
        this.masterTime = masterTime;
        this.version = version;
        final Map<Address, Integer> addressIndexes = new HashMap<Address, Integer>(memberInfos.size());
        int memberIndex = 0;
        for (MemberInfo memberInfo : memberInfos) {
            addMemberInfo(memberInfo, addressIndexes, memberIndex);
            memberIndex++;
        }
        setPartitions(partitions, addressIndexes);
        completedMigrations = migrationInfos != null ? migrationInfos : new ArrayList<MigrationInfo>(0);
    }

    protected void addMemberInfo(MemberInfo memberInfo, Map<Address, Integer> addressIndexes, int memberIndex) {
        members.add(memberIndex, memberInfo);
        addressIndexes.put(memberInfo.getAddress(), memberIndex);
    }

    protected void setPartitions(InternalPartition[] partitions, Map<Address, Integer> addressIndexes) {
        for (InternalPartition partition : partitions) {
            ShortPartitionInfo partitionInfo = new ShortPartitionInfo(partition.getPartitionId());
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                Address address = partition.getReplicaAddress(i);
                if (address == null) {
                    partitionInfo.addressIndexes[i] = -1;
                } else {
                    Integer knownIndex = addressIndexes.get(address);
                    partitionInfo.addressIndexes[i] = (knownIndex == null) ? -1 : knownIndex;
                }
            }
            partitionInfos.add(partitionInfo);
        }
    }

    public PartitionInfo[] getPartitions() {
        int size = partitionInfos.size();
        PartitionInfo[] result = new PartitionInfo[size];
        for (ShortPartitionInfo partitionInfo : partitionInfos) {
            Address[] replicas = new Address[InternalPartition.MAX_REPLICA_COUNT];
            int partitionId = partitionInfo.partitionId;
            result[partitionId] = new PartitionInfo(partitionId,replicas);
            int[] addressIndexes = partitionInfo.addressIndexes;
            for (int c = 0; c < addressIndexes.length; c++) {
                int index = addressIndexes[c];
                if (index != -1) {
                    replicas[c] = members.get(index).getAddress();
                }
            }
        }

        return result;
    }

    public List<MemberInfo> getMembers() {
        return members;
    }

    public long getMasterTime() {
        return masterTime;
    }

    public Address getEndpoint() {
        return endpoint;
    }

    void setEndpoint(final Address endpoint) {
        this.endpoint = endpoint;
    }

    public Collection<MigrationInfo> getCompletedMigrations() {
        return completedMigrations != null ? completedMigrations : Collections.<MigrationInfo>emptyList();
    }

    public void readData(ObjectDataInput in) throws IOException {
        masterTime = in.readLong();
        version = in.readInt();
        int size = in.readInt();
        final Map<Address, Integer> addressIndexes = new HashMap<Address, Integer>(size);
        int memberIndex = 0;
        while (size-- > 0) {
            MemberInfo memberInfo = new MemberInfo();
            memberInfo.readData(in);
            addMemberInfo(memberInfo, addressIndexes, memberIndex);
            memberIndex++;
        }
        int partitionCount = in.readInt();
        for (int i = 0; i < partitionCount; i++) {
            ShortPartitionInfo spi = new ShortPartitionInfo();
            spi.readData(in);
            partitionInfos.add(spi);
        }

        int k = in.readInt();
        if (k > 0) {
            completedMigrations = new ArrayList<MigrationInfo>(k);
            for (int i = 0; i < k; i++) {
                MigrationInfo cm = new MigrationInfo();
                cm.readData(in);
                completedMigrations.add(cm);
            }
        }
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(masterTime);
        out.writeInt(version);
        int memberSize = members.size();
        out.writeInt(memberSize);
        for (MemberInfo memberInfo : members) {
            memberInfo.writeData(out);
        }
        out.writeInt(partitionInfos.size());
        for (ShortPartitionInfo spi : partitionInfos) {
            spi.writeData(out);
        }

        if (completedMigrations != null) {
            int k = completedMigrations.size();
            out.writeInt(k);
            for (MigrationInfo cm : completedMigrations) {
                cm.writeData(out);
            }
        } else {
            out.writeInt(0);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PartitionRuntimeState [" + version + "]{\n");
        for (MemberInfo address : members) {
            sb.append(address).append('\n');
        }
        sb.append(", completedMigrations=").append(completedMigrations);
        sb.append('}');
        return sb.toString();
    }

    public int getVersion() {
        return version;
    }

    private static class ShortPartitionInfo implements DataSerializable {

        int partitionId;
        int[] addressIndexes = new int[InternalPartition.MAX_REPLICA_COUNT];

        ShortPartitionInfo(int partitionId) {
            this.partitionId = partitionId;
        }

        ShortPartitionInfo() {
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(partitionId);
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                out.writeInt(addressIndexes[i]);
            }
        }

        public void readData(ObjectDataInput in) throws IOException {
            partitionId = in.readInt();
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                addressIndexes[i] = in.readInt();
            }
        }
    }
}
