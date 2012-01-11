/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.impl.concurrentmap.MigrationRequestTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;

import java.util.*;
import java.util.logging.Level;

public class PartitionStateGenerator {

    private static ILogger logger = Logger.getLogger(PartitionStateGenerator.class.getName());

    public PartitionInfo[] initialize(final List<MemberImpl> members, final int partitionCount) {
        final PartitionInfo[] newState = new PartitionInfo[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            newState[i] = new PartitionInfo(i, null);
        }
        int tryCount = 0;
        do {
            arrange0(newState, members, partitionCount);
        } while (++tryCount < 3 && !test(newState, partitionCount, members.size()));
        return newState;
    }

    public PartitionInfo[] arrange(final PartitionInfo[] currentState,
                                   final List<MemberImpl> members,
                                   final int partitionCount,
                                   final Queue<MigrationRequestTask> migrationQueue,
                                   final Queue<MigrationRequestTask> replicaQueue) {
        final PartitionInfo[] newState = new PartitionInfo[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            newState[i] = currentState[i].copy();
        }
        int tryCount = 0;
        do {
            arrange0(newState, members, partitionCount);
        } while (++tryCount < 3 && !test(newState, partitionCount, members.size()));
        final int nodeSize = members.size();
        final int replicaCount = Math.min(nodeSize, PartitionInfo.MAX_REPLICA_COUNT);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            PartitionInfo currentPartition = currentState[partitionId];
            PartitionInfo newPartition = newState[partitionId];
            for (int replicaIndex = 0; replicaIndex < replicaCount; replicaIndex++) {
                Address currentOwner = currentPartition.getReplicaAddress(replicaIndex);
                Address newOwner = newPartition.getReplicaAddress(replicaIndex);
                if (currentOwner != null && newOwner != null && !currentOwner.equals(newOwner)) {
                    // migration owner or backup
                    MigrationRequestTask migrationRequestTask = new MigrationRequestTask(
                            partitionId, currentOwner, newOwner, replicaIndex, true);
                    migrationQueue.offer(migrationRequestTask);
                } else if (currentOwner == null && newOwner != null) {
                    // copy of a backup
                    currentOwner = currentPartition.getOwner();
                    MigrationRequestTask migrationRequestTask = new MigrationRequestTask(
                            partitionId, currentOwner, newOwner, replicaIndex, false);
                    replicaQueue.offer(migrationRequestTask);
                } else if (currentOwner != null && newOwner == null) {
                    // nop
                }
            }
        }
        return newState;
    }

    private void arrange0(final PartitionInfo[] state, final List<MemberImpl> members, final int partitionCount) {
        final int nodeSize = members.size();
        final int replicaCount = Math.min(nodeSize, PartitionInfo.MAX_REPLICA_COUNT);
        final int avgPartitionPerNode = partitionCount / nodeSize;
        final Random rand = new Random();
        // clear unused replica owners
        if (replicaCount < PartitionInfo.MAX_REPLICA_COUNT) {
            for (PartitionInfo partition : state) {
                for (int index = replicaCount - 1; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                    partition.setReplicaAddress(index, null);
                }
            }
        }
        // create partition registries for all members
        Map<Address, PartitionRegistry> partitionRegistryMap = new HashMap<Address, PartitionRegistry>(nodeSize);
        for (MemberImpl member : members) {
            partitionRegistryMap.put(member.getAddress(), new PartitionRegistry());
        }
        // initialize partition registry for each member
        for (PartitionInfo partition : state) {
            Address owner = null;
            PartitionRegistry partitionRegistry = null;
            for (int index = 0; index < replicaCount; index++) {
                if ((owner = partition.getReplicaAddress(index)) != null
                        && (partitionRegistry = partitionRegistryMap.get(owner)) != null) {
                    partitionRegistry.register(partition.getPartitionId());
                }
            }
        }
        for (int index = 0; index < replicaCount; index++) {
            final LinkedList<Integer> partitionsToArrange = new LinkedList<Integer>();
            int maxPartitionPerNode = avgPartitionPerNode + 1;
            int remainingPartitions = partitionCount - avgPartitionPerNode * nodeSize;
            // reset partition registry for each member
            for (MemberImpl member : members) {
                partitionRegistryMap.get(member.getAddress()).reset();
            }
            // register current replica partitions to members
            // if owner can not be found then add partition to arrange queue.
            for (PartitionInfo partition : state) {
                Address owner = null;
                PartitionRegistry partitionRegistry = null;
                if ((owner = partition.getReplicaAddress(index)) != null
                        && (partitionRegistry = partitionRegistryMap.get(owner)) != null) {
                    partitionRegistry.add(partition.getPartitionId());
                } else {
                    partitionsToArrange.add(partition.getPartitionId());
                }
            }
            // collect partitions to distribute members
            for (Address address : partitionRegistryMap.keySet()) {
                PartitionRegistry partitionRegistry = partitionRegistryMap.get(address);
                int size = partitionRegistry.size();
                if (size == maxPartitionPerNode) {
                    if (remainingPartitions > 0 && --remainingPartitions == 0) {
                        maxPartitionPerNode = avgPartitionPerNode;
                    }
                    continue;
                }
                int diff = (size - maxPartitionPerNode);
                for (int i = 0; i < diff; i++) {
                    int remove = rand.nextInt(partitionRegistry.size());
                    Integer partitionId = partitionRegistry.remove(remove);
                    partitionsToArrange.add(partitionId);
                }
            }
            Collections.shuffle(partitionsToArrange);
            // distribute partition replicas between under-loaded members
            for (Address address : partitionRegistryMap.keySet()) {
                PartitionRegistry partitionRegistry = partitionRegistryMap.get(address);
                final int queueSize = partitionsToArrange.size();
                int count = 0;
                while (partitionRegistry.size() < maxPartitionPerNode && count < queueSize) {
                    Integer partitionId = partitionsToArrange.poll();
                    count++;
                    if (partitionRegistry.contains(partitionId)) {
                        partitionsToArrange.offer(partitionId);
                    } else {
                        partitionRegistry.add(partitionId);
                        PartitionInfo p = state[partitionId];
                        p.setReplicaAddress(index, address);
                    }
                }
                if (remainingPartitions > 0 && --remainingPartitions == 0) {
                    maxPartitionPerNode = avgPartitionPerNode;
                }
            }
            // randomly distribute remaining partitions from process above
            while (!partitionsToArrange.isEmpty()) {
                Integer partitionId = partitionsToArrange.poll();
                MemberImpl member = members.get(rand.nextInt(members.size()));
                Address address = member.getAddress();
                PartitionRegistry partitionRegistry = partitionRegistryMap.get(address);
                if (!partitionRegistry.contains(partitionId)) {
                    partitionRegistry.add(partitionId);
                    PartitionInfo p = state[partitionId];
                    p.setReplicaAddress(index, address);
                } else {
                    partitionsToArrange.offer(partitionId);
                }
            }
        }
    }

    public static class PartitionTransfer {
        final int partitionId;
        final Address from;
        final Address to;
        final boolean migrate;

        public PartitionTransfer(int partitionId, Address from, Address to, boolean migrate) {
            super();
            this.partitionId = partitionId;
            this.from = from;
            this.to = to;
            this.migrate = migrate;
        }

        public String toString() {
            return "PartitionTransfer [partitionId=" + partitionId + ", from=" + from + ", to=" + to + ", migrate="
                    + migrate + "]";
        }
    }

    private class PartitionRegistry {
        Set<Integer> allPartitions = new HashSet<Integer>();
        List<Integer> currentPartitions = new ArrayList<Integer>();

        void add(Integer partitionId) {
            register(partitionId);
            currentPartitions.add(partitionId);
        }

        boolean register(Integer partitionId) {
            return allPartitions.add(partitionId);
        }

        boolean contains(Integer partitionID) {
            return allPartitions.contains(partitionID);
        }

        int size() {
            return currentPartitions.size();
        }

        void reset() {
            currentPartitions.clear();
        }

        Integer remove(int index) {
            Integer partitionId = currentPartitions.remove(index);
            allPartitions.remove(partitionId);
            return partitionId;
        }
    }

    private boolean test(PartitionInfo[] state, int partitionCount, int memberCount) {
        final int replicaCount = Math.min(memberCount, PartitionInfo.MAX_REPLICA_COUNT);
        final int avgPartitionPerNode = partitionCount / memberCount;
        Set<Address> set = new HashSet<Address>();
        Map<Address, List[]> map = new HashMap<Address, List[]>();
        for (PartitionInfo p : state) {
            for (int i = 0; i < replicaCount; i++) {
                Address owner = p.getReplicaAddress(i);
                if (set.contains(owner)) {
                    // Should not happen!
                    logger.log(Level.SEVERE, owner + " has owned multiple replicas of partition: " + p.getPartitionId());
                    return false;
                }
                set.add(owner);
                List[] ll = map.get(p.getReplicaAddress(i));
                if (ll == null) {
                    ll = new List[replicaCount];
                    for (int j = 0; j < replicaCount; j++) {
                        ll[j] = new ArrayList<Integer>();
                    }
                    map.put(owner, ll);
                }
                List<Integer> list = ll[i];
                list.add(p.getPartitionId());
            }
            set.clear();
        }
        for (Address a : map.keySet()) {
            List[] ll = map.get(a);
            for (int i = 0; i < ll.length; i++) {
                int partitionCountOfNode = ll[i].size();
                if ((partitionCountOfNode < avgPartitionPerNode / 2) || (partitionCountOfNode > avgPartitionPerNode * 2)) {
                    logger.log(Level.FINEST, "Replica: " + i + ", Owner: " + a + ", PartitonCount: " + partitionCountOfNode
                            + ", AvgPartitionCount: " + avgPartitionPerNode);
                    return false;
                }
            }
        }
        return true;
    }
}
