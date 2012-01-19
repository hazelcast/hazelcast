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

package com.hazelcast.impl.partition;

import java.util.*;
import java.util.logging.Level;

import com.hazelcast.impl.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;

class PartitionStateGeneratorImpl implements PartitionStateGenerator {
    
    private static ILogger logger = Logger.getLogger(PartitionStateGenerator.class.getName());
    private static final float RANGE_CHECK_RATIO = 1.3f; 
    
    private final MemberGroupFactory memberGroupFactory ;
    
    public PartitionStateGeneratorImpl(MemberGroupFactory nodeGroupFactory) {
        super();
        this.memberGroupFactory = nodeGroupFactory;
    }
    
    public PartitionInfo[] initialize(List<MemberImpl> members, int partitionCount) {
        final LinkedList<NodeGroup> groups = createNodeGroups(memberGroupFactory.createMemberGroups(members));
        return arrange(groups, partitionCount, new EmptyStateInitializer());
    }
    
    public PartitionInfo[] reArrange(PartitionInfo[] currentState, List<MemberImpl> members, int partitionCount,
            Queue<MigrationRequestTask> migrationQueue, Queue<MigrationRequestTask> replicaQueue) {
        final LinkedList<NodeGroup> groups = createNodeGroups(memberGroupFactory.createMemberGroups(members));
        PartitionInfo[] newState = arrange(groups, partitionCount, new CopyStateInitializer(currentState));
        finalizeArrangement(currentState, newState, Math.min(groups.size(), PartitionInfo.MAX_REPLICA_COUNT), 
                migrationQueue, replicaQueue);
        return newState;
    }
    
    private PartitionInfo[] arrange(final LinkedList<NodeGroup> groups, final int partitionCount, 
            final StateInitializer stateInitializer) {
        final PartitionInfo[] state = new PartitionInfo[partitionCount];
        stateInitializer.initialize(state);
        TestResult result = null;
        final int maxTries = 3;
        int tryCount = 0;
        do {
            tryArrange(state, groups, partitionCount);
            result = testArrangment(state, groups, partitionCount);
            if (result == TestResult.FAIL) {
                logger.log(Level.WARNING, "Error detected on partition arrangement! Try-count: " + tryCount);
                stateInitializer.initialize(state);
//                tryCount = 0;
            } else if (result == TestResult.RETRY) {
                tryCount++;
                logger.log(Level.INFO, "Re-trying partition arragement.. Count: " + tryCount);
            }
            
        } while (tryCount <= maxTries && result != TestResult.PASS);
        if (result == TestResult.FAIL) {
            logger.log(Level.SEVERE, "Failed to arrange partitions !!!");
        }
        return state;
    }
    
    private void finalizeArrangement(PartitionInfo[] currentState, PartitionInfo[] newState, int replicaCount,
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
                    // should not happen!
                    logger.log(Level.WARNING, "Something is wrong! Old owner valid but new owner is null!");
                }
            }
        }
    }
    
    private void tryArrange(final PartitionInfo[] state, final LinkedList<NodeGroup> groups, final int partitionCount) {
        final int groupSize = groups.size();
        final int replicaCount = Math.min(groupSize, PartitionInfo.MAX_REPLICA_COUNT);
        final int avgPartitionPerGroup = partitionCount / groupSize;
        final int maxPartitionPerGroup = avgPartitionPerGroup + 1;
        
        // reset partition before reuse
        for (NodeGroup nodeGroup : groups) {
            nodeGroup.resetPartitions();
        }
        
        // clear unused replica owners
        // initialize partition registry for each group
        for (PartitionInfo partition : state) {
            for (int index = 0; index < PartitionInfo.MAX_REPLICA_COUNT; index++) {
                if (index >= replicaCount) {
                    partition.setReplicaAddress(index, null);
                } else {
                    final Address owner = partition.getReplicaAddress(index);
                    boolean valid = false;
                    if (owner != null) {
                        for (NodeGroup nodeGroup : groups) {
                            if (nodeGroup.hasNode(owner)) {
                                valid = true;
                                nodeGroup.ownPartition(owner, index, partition.getPartitionId());
                                break;
                            }
                        }
                    }
                    if (!valid) {
                        partition.setReplicaAddress(index, null);
                    }
                }
            }
        }
        
        for (int index = 0; index < replicaCount; index++) {
            // partitions those are not bound to any node/group
            final LinkedList<Integer> freePartitions = new LinkedList<Integer>();
            // groups having partitions under average
            final LinkedList<NodeGroup> underLoadedGroups = new LinkedList<NodeGroup>();
            // groups having partitions over average
            final LinkedList<NodeGroup> overLoadedGroups = new LinkedList<NodeGroup>();
            // number of groups should have (average + 1) partitons
            int remainingPartitions = partitionCount - avgPartitionPerGroup * groupSize;
            
            // if owner of a can not be found then add partition to free partitions queue.
            for (PartitionInfo partition : state) {
                if (partition.getReplicaAddress(index) == null) {
                    freePartitions.add(partition.getPartitionId());
                }
            }
            Collections.shuffle(freePartitions);

            // detect under-loaded and over-loaded groups
            for (NodeGroup nodeGroup : groups) {
                int size = nodeGroup.getPartitonCount(index);
                if (size < avgPartitionPerGroup) {
                    underLoadedGroups.add(nodeGroup);
                } else if (size > avgPartitionPerGroup) { // maxPartitionPerGroup ?? 
                    overLoadedGroups.add(nodeGroup);
                }
            }
            
            // distribute free partitions among under-loaded groups
            int maxTries = freePartitions.size() * underLoadedGroups.size();
            int tries = 0;
            while (tries++ < maxTries && !freePartitions.isEmpty() && !underLoadedGroups.isEmpty()) {
                NodeGroup group = underLoadedGroups.poll();
                int size = freePartitions.size();
                for (int i = 0; i < size; i++) {
                    Integer partitionId = freePartitions.poll();
                    if (!group.addPartition(index, partitionId)) {
                        freePartitions.offer(partitionId);
                    } else {
                        break;
                    }
                }
                int count = group.getPartitonCount(index);
                if (remainingPartitions > 0 && count == maxPartitionPerGroup) {
                    if (--remainingPartitions == 0) {
                        // all (avg + 1) partitions owned groups are found
                        // if there is any group has avg number of partitions in under-loaded queue
                        // remove it.
                        Iterator<NodeGroup> underLoaded = underLoadedGroups.iterator();
                        while (underLoaded.hasNext()) {
                            if (underLoaded.next().getPartitonCount(index) >= avgPartitionPerGroup) {
                                underLoaded.remove();
                            }
                        }
                    }
                } else if ((remainingPartitions > 0 && count < maxPartitionPerGroup)
                        || (count < avgPartitionPerGroup)) {
                    underLoadedGroups.offer(group);
                }
            }
            
            {
                // if there are still free partitions those could not be distributed
                // to under-loaded groups then one-by-one distribute them among all groups
                // until queue is empty.
                maxTries = freePartitions.size() * groupSize * 10;
                tries = 0;
                Integer partitionId = freePartitions.poll(); 
                while (partitionId != null && tries++ < maxTries) {
                    NodeGroup group = groups.poll();
                    if (group.addPartition(index, partitionId)) {
                        partitionId = freePartitions.poll();
                    }
                    groups.offer(group);
                }
            }
            
            // TODO: what if there are still free partitions?
            
            // iterate through over-loaded groups' partitions and distribute them to under-loaded groups.
            maxTries = underLoadedGroups.size() * overLoadedGroups.size() * 10;
            tries = 0;
            int expectedPartitionCount = remainingPartitions > 0 ? maxPartitionPerGroup : avgPartitionPerGroup;
            while (tries++ < maxTries && !underLoadedGroups.isEmpty()) {
                NodeGroup toGroup = underLoadedGroups.poll();
                Iterator<NodeGroup> overLoadedGroupsIter = overLoadedGroups.iterator();
                while (overLoadedGroupsIter.hasNext()) {
                    NodeGroup fromGroup = overLoadedGroupsIter.next();
                    final Iterator<Integer> partitionsIter = fromGroup.getPartitionsIterator(index);
                    while (partitionsIter.hasNext() 
                            && fromGroup.getPartitonCount(index) > expectedPartitionCount
                            && toGroup.getPartitonCount(index) < expectedPartitionCount) {
                        Integer partitionId = partitionsIter.next();
                        if (toGroup.addPartition(index, partitionId)) {
                            partitionsIter.remove();
                        }
                    }
                    
                    int fromCount = fromGroup.getPartitonCount(index);
                    if (remainingPartitions > 0 && fromCount == maxPartitionPerGroup) {
                        if (--remainingPartitions == 0) {
                            expectedPartitionCount = avgPartitionPerGroup;
                        }
                    }
                    if (fromCount <= expectedPartitionCount) {
                        overLoadedGroupsIter.remove();
                    }
                    
                    int toCount = toGroup.getPartitonCount(index);
                    if (remainingPartitions > 0 && toCount == maxPartitionPerGroup) {
                        if (--remainingPartitions == 0) {
                            expectedPartitionCount = avgPartitionPerGroup;
                        }
                    }
                    if (toCount >= expectedPartitionCount) {
                        break;
                    }
                }
                if (toGroup.getPartitonCount(index) < avgPartitionPerGroup/* && !underLoadedGroups.contains(toGroup)*/) {
                    underLoadedGroups.offer(toGroup);
                }
            }
            
            // post process each group's partition table (distribute partitions added to group to nodes 
            // and balance load of partition ownership s in group) and save partition ownerships to 
            // cluster partition state table.
            for (NodeGroup group : groups) {
                group.postProcessPartitionTable(index);
                for (Address address : group.getNodes()) {
                    PartitionTable table = group.getPartitionTable(address);
                    Set<Integer> set = table.getPartitions(index);
                    for (Integer partitionId : set) {
                        state[partitionId.intValue()].setReplicaAddress(index, address);
                    }
                }
            }
        }
    }
    
    private LinkedList<NodeGroup> createNodeGroups(Collection<MemberGroup> memberGroups) {
        LinkedList<NodeGroup> nodeGroups = new LinkedList<NodeGroup>();
        for (MemberGroup memberGroup : memberGroups) {
            final NodeGroup nodeGroup ;
            if (memberGroup.size() == 0) {
                continue;
            }
            if (memberGroup instanceof SingleMemberGroup || memberGroup.size() == 1) {
                nodeGroup = new SingleNodeGroup();
                nodeGroup.addNode(memberGroup.iterator().next().getAddress());
            } else {
                nodeGroup = new DefaultNodeGroup();
                Iterator<MemberImpl> iter = memberGroup.iterator();
                while (iter.hasNext()) {
                    nodeGroup.addNode(iter.next().getAddress());
                }
            }
            nodeGroups.add(nodeGroup);
        }
        return nodeGroups;
    }
    
    private TestResult testArrangment(PartitionInfo[] state, Collection<NodeGroup> groups, int partitionCount) {
        final float ratio = RANGE_CHECK_RATIO; 
        final int avgPartitionPerGroup = partitionCount / groups.size();
        final int replicaCount = Math.min(groups.size(), PartitionInfo.MAX_REPLICA_COUNT);
        final Set<Address> set = new HashSet<Address>();
        for (PartitionInfo p : state) {
            for (int i = 0; i < replicaCount; i++) {
                Address owner = p.getReplicaAddress(i);
                if (owner == null) {
                    logger.log(Level.WARNING, "Partition-Arrangement-Test: Owner is null !!! => partition: " 
                            + p.getPartitionId() + " replica: " + i);
                    return TestResult.FAIL;
                }
                if (set.contains(owner)) {
                    // Should not happen!
                    logger.log(Level.WARNING, "Partition-Arrangement-Test: " + 
                            owner + " has owned multiple replicas of partition: " + p.getPartitionId() + " replica: " + i);
                    return TestResult.FAIL;
                }
                set.add(owner);
            }
            set.clear();
        }
        
        for (NodeGroup group : groups) {
            for (int i = 0; i < replicaCount; i++) {
                int partitionCountOfGroup = group.getPartitonCount(i);
                if ((partitionCountOfGroup < avgPartitionPerGroup / ratio)
                        || (partitionCountOfGroup > avgPartitionPerGroup * ratio)) {
                    logger.log(Level.FINEST, "Replica: " + i + ", PartitonCount: "
                            + partitionCountOfGroup + ", AvgPartitionCount: " + avgPartitionPerGroup);
                    return TestResult.RETRY;
                }
            }
        }
        return TestResult.PASS;
    }
    
    // ----- INNER CLASSES -----
    
    private interface StateInitializer {
        void initialize(PartitionInfo[] state);
    }
    
    private class EmptyStateInitializer implements StateInitializer {
        public void initialize(PartitionInfo[] state) {
            for (int i = 0; i < state.length; i++) {
                state[i] = new PartitionInfo(i);
            }
        }
    }
    
    private class CopyStateInitializer implements StateInitializer {
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

    private interface NodeGroup {
        void addNode(Address address);
        boolean hasNode(Address address);
        Set<Address> getNodes();
        PartitionTable getPartitionTable(Address address);
        void resetPartitions();
        int getPartitonCount(int index);
        boolean containsPartition(Integer partitionId);
        void ownPartition(Address address, int index, Integer partitionId);
        boolean addPartition(int replicaIndex, Integer partitionId);
        Iterator<Integer> getPartitionsIterator(int index);
        boolean removePartition(int index, Integer partitionId);
        void postProcessPartitionTable(int index);
    }
    
    private class DefaultNodeGroup implements NodeGroup {
        final PartitionTable groupPartitionTable = new PartitionTable();
        final Map<Address, PartitionTable> nodePartitionTables = new HashMap<Address, PartitionTable>(); 
        final Set<Address> nodes = nodePartitionTables.keySet();
        final Collection<PartitionTable> nodeTables = nodePartitionTables.values();
        final LinkedList<Integer> partitionQ = new LinkedList<Integer>();
        
        public void addNode(Address address) {
            nodePartitionTables.put(address, new PartitionTable());
        }
        public boolean hasNode(Address address) {
            return nodes.contains(address);
        }
        public Set<Address> getNodes() {
            return nodes;
        }
        public PartitionTable getPartitionTable(Address address) {
            return nodePartitionTables.get(address);
        }
        public void resetPartitions() {
            groupPartitionTable.reset();
            partitionQ.clear();
            for (PartitionTable table : nodeTables) {
                table.reset();
            }
        }
        public int getPartitonCount(int index) {
            return groupPartitionTable.size(index);
        }
        public boolean containsPartition(Integer partitionId) {
            return groupPartitionTable.contains(partitionId);
        }
        public void ownPartition(Address address, int index, Integer partitionId) {
            if (!hasNode(address)) {
                String error = "Address does not belong to this group: " + address.toString();
                logger.log(Level.SEVERE, error);
                throw new IllegalArgumentException(error);
            }
            if (containsPartition(partitionId)) {
                String error = "Partition[" + partitionId + "] is already owned by this group! " +
                    "Duplicate!";
                logger.log(Level.SEVERE, error);
                throw new IllegalArgumentException(error);
            }
            groupPartitionTable.add(index, partitionId);
            nodePartitionTables.get(address).add(index, partitionId);
        }
        public boolean addPartition(int replicaIndex, Integer partitionId) {
            if (containsPartition(partitionId)) {
                return false;
            }
            if (groupPartitionTable.add(replicaIndex, partitionId)) {
                partitionQ.add(partitionId);
                return true;
            }
            return false;
        }
        public Iterator<Integer> getPartitionsIterator(final int index) {
            final Iterator<Integer> iter = groupPartitionTable.getPartitions(index).iterator();
            return new Iterator<Integer>() {
                Integer current = null;
                public boolean hasNext() {
                    return iter.hasNext();
                }
                public Integer next() {
                    return (current = iter.next());
                }
                public void remove() {
                    iter.remove();
                    doRemovePartition(index, current);
                }
            };
        }
        public boolean removePartition(int index, Integer partitionId) {
            if (groupPartitionTable.remove(index, partitionId)) {
                doRemovePartition(index, partitionId);
                return true;
            }
            return false;
        }
        private void doRemovePartition(int index, Integer partitionId) {
            for (PartitionTable table : nodeTables) {
                if (table.remove(index, partitionId)) {
                    break;
                }
            }
        }
        public void postProcessPartitionTable(int index) {
            if (nodes.size() == 1) {
                PartitionTable table = nodeTables.iterator().next();
                while (!partitionQ.isEmpty()) {
                    table.add(index, partitionQ.poll());
                }
            } else {
                int totalCount = getPartitonCount(index);
                int avgCount = totalCount / nodes.size();
                List<PartitionTable> underLoadedStates = new LinkedList<PartitionTable>();
                for (PartitionTable table : nodeTables) {
                    Set<Integer> partitions = table.getPartitions(index);
                    if (partitions.size() > avgCount) {
                        Iterator<Integer> iter = partitions.iterator();
                        while (partitions.size() > avgCount) {
                            Integer partitionId = iter.next();
                            iter.remove();
                            partitionQ.add(partitionId);
                        }
                    } else {
                        underLoadedStates.add(table);
                    }
                }
                if (!partitionQ.isEmpty()) {
                    for (PartitionTable table : underLoadedStates) {
                        while (table.size(index) < avgCount) {
                            table.add(index, partitionQ.poll());
                        }
                    }
                }
                while (!partitionQ.isEmpty()) {
                    for (PartitionTable table : nodeTables) {
                        table.add(index, partitionQ.poll());
                        if (partitionQ.isEmpty()) {
                            break;
                        }
                    }
                }
            }
        }
        @Override
        public String toString() {
            return "DefaultNodeGroupRegistry [nodes=" + nodes + "]";
        }
    }
    
    private class SingleNodeGroup implements NodeGroup {
        final PartitionTable nodeTable = new PartitionTable();
        Address address = null; 
        Set<Address> nodes ;
        public void addNode(Address addr) {
            if (address != null) {
                throw new IllegalArgumentException("Single node group already has an address => " + address);
            }
            this.address = addr;
            nodes = Collections.singleton(address);
        }
        public boolean hasNode(Address address) {
            return this.address != null && this.address.equals(address);
        }
        public Set<Address> getNodes() {
            return nodes;
        }
        public PartitionTable getPartitionTable(Address address) {
            return hasNode(address) ? nodeTable : null;
        }
        public void resetPartitions() {
            nodeTable.reset();
        }
        public int getPartitonCount(int index) {
            return nodeTable.size(index);
        }
        public boolean containsPartition(Integer partitionId) {
            return nodeTable.contains(partitionId);
        }
        public void ownPartition(Address address, int index, Integer partitionId) {
            if (!hasNode(address)) {
                String error = address + " is different from this node's " + this.address;
                logger.log(Level.SEVERE, error);
                throw new IllegalArgumentException(error);
            }
            if (containsPartition(partitionId)) {
                String error = "Partition[" + partitionId + "] is already owned by this node " +
                    address + "! Duplicate!";
                logger.log(Level.SEVERE, error);
                throw new IllegalArgumentException(error);
            }
            nodeTable.add(index, partitionId);
        }
        public boolean addPartition(int replicaIndex, Integer partitionId) {
            if (containsPartition(partitionId)) {
                return false;
            }
            return nodeTable.add(replicaIndex, partitionId);
        }
        public Iterator<Integer> getPartitionsIterator(final int index) {
            return nodeTable.getPartitions(index).iterator();
        }
        public boolean removePartition(int index, Integer partitionId) {
            return nodeTable.remove(index, partitionId);
        }
        public void postProcessPartitionTable(int index) {
        }
        @Override
        public String toString() {
            return "SingleNodeGroupRegistry [address=" + address + "]";
        }
    }
    
    @SuppressWarnings("unchecked")
    private class PartitionTable {
        final Set<Integer>[] partitions = new Set[PartitionInfo.MAX_REPLICA_COUNT];
        Set<Integer> getPartitions(int index) {
            check(index);
            Set<Integer> set = partitions[index];
            if (set == null) {
                set = new HashSet<Integer>();
                partitions[index] = set;
            }
            return set;
        }
        boolean add(int index, Integer partitionId) {
            return getPartitions(index).add(partitionId);
        }
        boolean contains(int index, Integer partitionId) {
            return getPartitions(index).contains(partitionId);
        }
        boolean contains(Integer partitionId) {
            for (int i = 0; i < partitions.length; i++) {
                Set<Integer> set = partitions[i];
                if (set != null && set.contains(partitionId)) {
                    return true;
                }
            }
            return false;
        }
        boolean remove(int index, Integer partitionId) {
            return getPartitions(index).remove(partitionId);
        }
        int size(int index) {
            return getPartitions(index).size();
        }
        void reset() {
            for (int i = 0; i < partitions.length; i++) {
                Set<Integer> set = partitions[i];
                if (set != null) {
                    set.clear();
                }
            }
        }
        private void check(int index) {
            if (index < 0 || index >= PartitionInfo.MAX_REPLICA_COUNT) {
                throw new ArrayIndexOutOfBoundsException(index);
            }
        }
    }

}
