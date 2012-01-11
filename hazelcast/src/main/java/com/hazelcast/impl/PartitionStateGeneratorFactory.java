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

import java.util.*;
import java.util.logging.Level;

import com.hazelcast.impl.concurrentmap.MigrationRequestTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;

public final class PartitionStateGeneratorFactory {
    
    private static ILogger logger = Logger.getLogger(PartitionStateGenerator.class.getName());
    
    public static PartitionStateGenerator newRandomPartitionStateGenerator() {
        return new RandomPartitionStateGenerator();
    }
    
    public static PartitionStateGenerator newHostAwarePartitionStateGenerator() {
        return new HostAwarePartitionStateGenerator();
    }
    
    private static class RandomPartitionStateGenerator implements PartitionStateGenerator {
        public PartitionInfo[] initialize(List<MemberImpl> members, int partitionCount) {
            return arrange(members, partitionCount, new PerNodePartitionRegistry(), new EmptyStateInitializer());
        }

        public PartitionInfo[] reArrange(PartitionInfo[] currentState, List<MemberImpl> members, int partitionCount,
                Queue<MigrationRequestTask> migrationQueue, Queue<MigrationRequestTask> replicaQueue) {
            final ClusterPartitionRegistry registry = new PerNodePartitionRegistry();
            PartitionInfo[] newState = arrange(members, partitionCount, registry, new CopyStateInitializer(currentState));
            finalizeArrangement(currentState, newState, members.size(), registry.getMaxReplicaCount(), migrationQueue, replicaQueue);
            return newState;
        }
        
        public GeneratorType getType() {
            return GeneratorType.RANDOM;
        }
    }
    
    private static class HostAwarePartitionStateGenerator implements PartitionStateGenerator {
        public PartitionInfo[] initialize(List<MemberImpl> members, int partitionCount) {
            return arrange(members, partitionCount, new PerHostPartitionRegistry(), new EmptyStateInitializer());
        }

        public PartitionInfo[] reArrange(PartitionInfo[] currentState, List<MemberImpl> members, int partitionCount,
                Queue<MigrationRequestTask> migrationQueue, Queue<MigrationRequestTask> replicaQueue) {
            final ClusterPartitionRegistry registry = new PerHostPartitionRegistry();
            PartitionInfo[] newState = arrange(members, partitionCount, registry, new CopyStateInitializer(currentState));
            finalizeArrangement(currentState, newState, members.size(), registry.getMaxReplicaCount(), migrationQueue, replicaQueue);
            return newState;
        }
        
        public GeneratorType getType() {
            return GeneratorType.HOST_AWARE;
        }
    }
    
    private static interface StateInitializer {
        void initialize(PartitionInfo[] state);
    }
    
    private static class EmptyStateInitializer implements StateInitializer {
        public void initialize(PartitionInfo[] state) {
            for (int i = 0; i < state.length; i++) {
                state[i] = new PartitionInfo(i);
            }
        }
    }
    
    private static class CopyStateInitializer implements StateInitializer {
        private final PartitionInfo[] currentState;
        CopyStateInitializer(PartitionInfo[] currentState) {
            this.currentState = currentState;
        }
        public void initialize(PartitionInfo[] state) {
            if (state.length != currentState.length) {
                throw new IllegalArgumentException("Partition counts do not match!");
            }
            for (int i = 0; i < state.length; i++) {
                state[i] = currentState[i].copy();
            }
        }
    }
    
    private enum TestResult {
        PASS, RETRY, FAIL  
    }
    
    private static PartitionInfo[] arrange(final List<MemberImpl> members, 
            final int partitionCount, final ClusterPartitionRegistry clusterPartitionRegistry, StateInitializer stateInitializer) {
        final PartitionInfo[] state = new PartitionInfo[partitionCount];
        stateInitializer.initialize(state);
        int tryCount = 0;
        TestResult result = null;
        boolean failed = false;
        do {
            doArrange(state, members, partitionCount, clusterPartitionRegistry);
            result = testArrangment(state, partitionCount, members.size(), clusterPartitionRegistry.getMaxReplicaCount());
            
            if (result == TestResult.FAIL) {
                logger.log(Level.WARNING, "Error detected on partition arrangement! Try-count: " + tryCount);
                stateInitializer.initialize(state);
                failed = true;
            } else if (failed && result != TestResult.FAIL) {
                logger.log(Level.INFO, "Successfully re-rearranged partitions after an error...");
                break;
            } else {
                tryCount++;
                logger.log(Level.FINEST, "Re-trying partition arragement.. Count: " + tryCount);
            }
            
        } while (tryCount < 3 && result != TestResult.PASS);
        return state;
    }
    
    private static void finalizeArrangement(PartitionInfo[] currentState, PartitionInfo[] newState, int nodeCount, int replicaCount,
            Queue<MigrationRequestTask> migrationQueue, Queue<MigrationRequestTask> replicaQueue) {
        final int partitionCount = currentState.length;
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
    }
    
    private static void doArrange(final PartitionInfo[] state, final List<MemberImpl> members, final int partitionCount, 
            final ClusterPartitionRegistry clusterPartitionRegistry) {
        // create partition registries for all members
        clusterPartitionRegistry.reset();
        for (MemberImpl member : members) {
            clusterPartitionRegistry.init(member.getAddress());
        }
        
        final int nodeSize = members.size();
        final int replicaCount = clusterPartitionRegistry.getMaxReplicaCount();
        final int avgPartitionPerNode = partitionCount / nodeSize;
        final Random rand = new Random();
        
        // clear unused replica owners
        if (replicaCount < PartitionInfo.MAX_REPLICA_COUNT) {
            for (PartitionInfo partition : state) {
                for (int index = replicaCount-1; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                    partition.setReplicaAddress(index, null);
                }
            }
        }
        
        // initialize partition registry for each member
        for (PartitionInfo partition : state) {
            Address owner = null;
            for (int index = 0; index < replicaCount; index++) {
                if ((owner = partition.getReplicaAddress(index)) != null) {
                    clusterPartitionRegistry.register(owner, partition.getPartitionId());
                }
            }
        }
        
        for (int index = 0; index < replicaCount; index++) {
            final LinkedList<Integer> partitionsToArrange = new LinkedList<Integer>();
            int maxPartitionPerNode = avgPartitionPerNode + 1;
            int remainingPartitions = partitionCount - avgPartitionPerNode * nodeSize;
            
            // clear partition registry for each member
            clusterPartitionRegistry.clearAll();
            
            // register current replica partitions to members
            // if owner can not be found then add partition to arrange queue.
            for (PartitionInfo partition : state) {
                Address owner = null;
                if ((owner = partition.getReplicaAddress(index)) != null) {
                    clusterPartitionRegistry.add(owner, partition.getPartitionId());
                } else {
                    partitionsToArrange.add(partition.getPartitionId());
                }
            }
            
            // collect partitions to distribute members
            for (Address address : clusterPartitionRegistry.getAddresses()) {
                int size = clusterPartitionRegistry.size(address);
                if (size == maxPartitionPerNode) {
                    if (remainingPartitions > 0 && --remainingPartitions == 0) {
                        maxPartitionPerNode = avgPartitionPerNode;
                    }
                    continue;
                }
                
                int diff = (size - maxPartitionPerNode);
                for (int i = 0; i < diff; i++) {
                    int remove = rand.nextInt(clusterPartitionRegistry.size(address));
                    Integer partitionId = clusterPartitionRegistry.remove(address, remove);
                    partitionsToArrange.add(partitionId);
                }
            }
            
            Collections.shuffle(partitionsToArrange);
            
            // distribute partition replicas between under-loaded members 
            for (Address address : clusterPartitionRegistry.getAddresses()) {
                final int queueSize = partitionsToArrange.size();
                int count = 0;
                while (clusterPartitionRegistry.size(address) < maxPartitionPerNode && count < queueSize) {
                    Integer partitionId = partitionsToArrange.poll();
                    count++;
                    if (clusterPartitionRegistry.contains(address, partitionId)) {
                        partitionsToArrange.offer(partitionId);
                    } else {
                        clusterPartitionRegistry.add(address, partitionId);
                        PartitionInfo p = state[partitionId];
                        p.setReplicaAddress(index, address);
                    }
                }
                
                if (remainingPartitions > 0 && --remainingPartitions == 0) {
                    maxPartitionPerNode = avgPartitionPerNode;
                }
            }
           
            // randomly distribute remaining partitions from process above
            final int maxTries = partitionsToArrange.size() * nodeSize;
            int tries = 0;
            while (tries++ < maxTries && !partitionsToArrange.isEmpty()) {
                Integer partitionId = partitionsToArrange.poll();
                MemberImpl member = members.get(rand.nextInt(members.size()));
                Address address = member.getAddress();
                if (!clusterPartitionRegistry.contains(address, partitionId)) {
                    clusterPartitionRegistry.add(address, partitionId);
                    PartitionInfo p = state[partitionId];
                    p.setReplicaAddress(index, address);
                } else {
                    partitionsToArrange.offer(partitionId);
                }
            }
        }
    }
    
    private static interface ClusterPartitionRegistry {
        void init(Address address);
        Set<Address> getAddresses();
        void add(Address address, Integer partitionId);
        boolean register(Address address, Integer partitionId);
        boolean contains(Address address, Integer partitionId);
        int size(Address address);
        void clear(Address address);
        void clearAll();
        void reset();
        Integer remove(Address address, int index);
        int getMaxReplicaCount();
    }
    
    private abstract static class ClusterPartitionRegistrySupport implements ClusterPartitionRegistry {
        final Map<Address, List<Integer>> currentPartitionsMap = new HashMap<Address, List<Integer>>();
        public void add(Address address, Integer partitionId) {
            register(address, partitionId);
            getCurrentPartitions(address).add(partitionId);
        }
        public boolean register(Address address, Integer partitionId) {
            return getAllPartitions(address).add(partitionId);
        }
        public boolean contains(Address address, Integer partitionId) {
            return getAllPartitions(address).contains(partitionId);
        }
        public int size(Address address) {
            return getCurrentPartitions(address).size();
        }
        public void clear(Address address) {
            getCurrentPartitions(address).clear();
        }
        public void clearAll() {
            for (Address address : getAddresses()) {
                clear(address);
            }
        }
        public Integer remove(Address address, int index) {
            Integer partitionId = getCurrentPartitions(address).remove(index);
            getAllPartitions(address).remove(partitionId);
            return partitionId;
        }
        protected List<Integer> getCurrentPartitions(Address address) {
            List<Integer> list = currentPartitionsMap.get(address);
            if (list == null) {
                list = new ArrayList<Integer>();
                currentPartitionsMap.put(address, list);
            }
            return list;
        }
        protected abstract Set<Integer> getAllPartitions(Address address) ;
    }
    
    private static class PerNodePartitionRegistry extends ClusterPartitionRegistrySupport {
        final Map<Address, Set<Integer>> allPartitionsMap = new HashMap<Address, Set<Integer>>();
        public void init(Address address) {
            getAllPartitions(address);
        }
        public Set<Address> getAddresses() {
            return allPartitionsMap.keySet();
        }
        protected Set<Integer> getAllPartitions(Address address) {
            Set<Integer> set = allPartitionsMap.get(address);
            if (set == null) {
                set = new HashSet<Integer>();
                allPartitionsMap.put(address, set);
            }
            return set;
        }
        public int getMaxReplicaCount() {
            return Math.min(getAddresses().size(), PartitionInfo.MAX_REPLICA_COUNT);
        }
        public void reset() {
            allPartitionsMap.clear();
            currentPartitionsMap.clear();
        }
    }
    
    private static class PerHostPartitionRegistry extends ClusterPartitionRegistrySupport {
        final Set<Address> addresses = new HashSet<Address>();
        final Map<String, Set<Integer>> allPartitionsMap = new HashMap<String, Set<Integer>>();
        public void init(Address address) {
            addresses.add(address);
            getAllPartitions(address);
        }
        public Set<Address> getAddresses() {
            return addresses;
        }
        protected Set<Integer> getAllPartitions(Address address) {
            Set<Integer> set = allPartitionsMap.get(address.getHost());
            if (set == null) {
                set = new HashSet<Integer>();
                allPartitionsMap.put(address.getHost(), set);
            }
            return set;
        }
        public int getMaxReplicaCount() {
            return Math.min(allPartitionsMap.keySet().size(), PartitionInfo.MAX_REPLICA_COUNT);
        }
        public void reset() {
            addresses.clear();
            allPartitionsMap.clear();
            currentPartitionsMap.clear();
        }
    }

    private static TestResult testArrangment(PartitionInfo[] state, int partitionCount, int nodeSize, int replicaCount) {
        final int avgPartitionPerNode = partitionCount / nodeSize;
        final Set<Address> set = new HashSet<Address>();
        final Map<Address, List[]> map = new HashMap<Address, List[]>();
        for (PartitionInfo p : state) {
            for (int i = 0; i < replicaCount; i++) {
                Address owner = p.getReplicaAddress(i);
                if (owner == null) {
                    logger.log(Level.SEVERE, "Partition-Arrangementasaswner  sd is null !!! => partition: " + p.getPartitionId() + " replica: " + i);
                    return TestResult.FAIL;
                }
                if (set.contains(owner)) {
                    // Should not happen!
                    logger.log(Level.SEVERE, owner + " has owned multiple replicas of partition: " + p.getPartitionId() + " replica: " + i);
                    return TestResult.FAIL;
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
                    logger.log(Level.INFO, "Replica: " + i + ", Owner: " + a + ", PartitonCount: " + partitionCountOfNode 
                            + ", AvgPartitionCount: " + avgPartitionPerNode);
                    return TestResult.RETRY;
                }
            }
        }
        return TestResult.PASS;
    }
    
    private PartitionStateGeneratorFactory() {}
}
