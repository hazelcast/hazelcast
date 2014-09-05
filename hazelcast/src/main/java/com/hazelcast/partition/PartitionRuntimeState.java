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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PartitionRuntimeState implements DataSerializable {

    protected final ArrayList<MemberInfo> members = new ArrayList<MemberInfo>(100);

    private final Collection<ShortPartitionInfo> partitionInfos = new LinkedList<ShortPartitionInfo>();
    private ILogger logger;
    private long masterTime = Clock.currentTimeMillis();
    private int version;
    private Collection<MigrationInfo> completedMigrations;
    private Address endpoint;

    public PartitionRuntimeState() {
    }

    public PartitionRuntimeState(ILogger logger,
                                 Collection<MemberInfo> memberInfos,
                                 InternalPartition[] partitions,
                                 Collection<MigrationInfo> migrationInfos,
                                 long masterTime, int version) {
        this.logger = logger;
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
        List<String> unmatchAddresses = new LinkedList<String>();
        for (InternalPartition partition : partitions) {
            ShortPartitionInfo partitionInfo = new ShortPartitionInfo(partition.getPartitionId());
            for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                Address address = partition.getReplicaAddress(index);
                if (address == null) {
                    partitionInfo.addressIndexes[index] = -1;
                } else {
                    Integer knownIndex = addressIndexes.get(address);

                    if (knownIndex == null && index == 0) {
                        unmatchAddresses.add(address + " -> " + partition);
                    }
                    if (knownIndex == null) {
                        partitionInfo.addressIndexes[index] = -1;
                    } else {
                        partitionInfo.addressIndexes[index] = knownIndex;
                    }
                }
            }
            partitionInfos.add(partitionInfo);
        }

        if (!unmatchAddresses.isEmpty()) {
            // it can happen that the primary address at any given moment is not known,
            // most probably because master node has updated/published the partition table yet
            // or partition table update is not received yet.
            logger.warning("Unknown owner addresses in partition state! "
                    + "(Probably they have recently joined to or left the cluster.) " + unmatchAddresses);
        }
    }

    public PartitionInfo[] getPartitions() {
        int size = partitionInfos.size();
        PartitionInfo[] result = new PartitionInfo[size];
        for (ShortPartitionInfo partitionInfo : partitionInfos) {
            Address[] replicas = new Address[InternalPartition.MAX_REPLICA_COUNT];
            int partitionId = partitionInfo.partitionId;
            result[partitionId] = new PartitionInfo(partitionId, replicas);
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

    public void setEndpoint(final Address endpoint) {
        this.endpoint = endpoint;
    }

    public Collection<MigrationInfo> getCompletedMigrations() {
        return completedMigrations != null ? completedMigrations : Collections.<MigrationInfo>emptyList();
    }

    @Override
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

    @Override
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
        final int[] addressIndexes = new int[InternalPartition.MAX_REPLICA_COUNT];

        ShortPartitionInfo(int partitionId) {
            this.partitionId = partitionId;
        }

        ShortPartitionInfo() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(partitionId);
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                out.writeInt(addressIndexes[i]);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            partitionId = in.readInt();
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                addressIndexes[i] = in.readInt();
            }
        }
    }
}
