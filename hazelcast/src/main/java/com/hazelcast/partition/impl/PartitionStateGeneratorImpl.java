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

package com.hazelcast.partition.impl;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.membergroup.MemberGroup;
import com.hazelcast.partition.membergroup.SingleMemberGroup;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public final class PartitionStateGeneratorImpl implements PartitionStateGenerator {

    private static final ILogger LOGGER = Logger.getLogger(PartitionStateGenerator.class);

    private static final int DEFAULT_RETRY_MULTIPLIER = 10;

    private static final float RANGE_CHECK_RATIO = 1.1f;
    private static final int MAX_RETRY_COUNT = 3;
    private static final int AGGRESSIVE_RETRY_THRESHOLD = 1;
    private static final int AGGRESSIVE_INDEX_THRESHOLD = 3;
    private static final int MIN_AVG_OWNER_DIFF = 3;

    @Override
    public Address[][] initialize(Collection<MemberGroup> memberGroups, int partitionCount) {
        Queue<NodeGroup> nodeGroups = createNodeGroups(memberGroups);
        if (nodeGroups.size() == 0) {
            return null;
        }
        return arrange(nodeGroups, partitionCount, new EmptyStateInitializer());
    }

    @Override
    public Address[][] reArrange(Collection<MemberGroup> memberGroups, InternalPartition[] currentState) {
        Queue<NodeGroup> nodeGroups = createNodeGroups(memberGroups);
        if (nodeGroups.size() == 0) {
            return null;
        }
        return arrange(nodeGroups, currentState.length, new CopyStateInitializer(currentState));
    }

    private Address[][] arrange(Queue<NodeGroup> groups, int partitionCount, StateInitializer stateInitializer) {
        Address[][] state = new Address[partitionCount][];
        stateInitializer.initialize(state);
        TestResult result = null;
        int tryCount = 0;
        while (tryCount < MAX_RETRY_COUNT && result != TestResult.PASS) {
            boolean aggressive = tryCount >= AGGRESSIVE_RETRY_THRESHOLD;
            tryArrange(state, groups, partitionCount, aggressive);
            result = testArrangement(state, groups, partitionCount);
            if (result == TestResult.FAIL) {
                LOGGER.warning("Error detected on partition arrangement! Try-count: " + tryCount);
                stateInitializer.initialize(state);
            } else if (result == TestResult.RETRY) {
                tryCount++;
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Re-trying partition arrangement.. Count: " + tryCount);
                }
            }
        }
        if (result == TestResult.FAIL) {
            LOGGER.severe("Failed to arrange partitions !!!");
        }
        return state;
    }

    private void tryArrange(Address[][] state, Queue<NodeGroup> groups, int partitionCount, boolean aggressive) {
        int groupSize = groups.size();
        int replicaCount = Math.min(groupSize, InternalPartition.MAX_REPLICA_COUNT);
        int avgPartitionPerGroup = partitionCount / groupSize;
        // clear unused replica owners
        // initialize partition registry for each group
        initializeGroupPartitions(state, groups, replicaCount, aggressive);
        for (int index = 0; index < replicaCount; index++) {
            // partitions those are not bound to any node/group
            Queue<Integer> freePartitions = getUnownedPartitions(state, index);
            // groups having partitions under average
            Queue<NodeGroup> underLoadedGroups = new LinkedList<NodeGroup>();
            // groups having partitions over average
            List<NodeGroup> overLoadedGroups = new LinkedList<NodeGroup>();
            // number of groups should have (average + 1) partitions
            int plusOneGroupCount = partitionCount - avgPartitionPerGroup * groupSize;
            // determine under-loaded and over-loaded groups
            for (NodeGroup nodeGroup : groups) {
                int size = nodeGroup.getPartitionCount(index);
                if (size < avgPartitionPerGroup) {
                    underLoadedGroups.add(nodeGroup);
                } else if (size > avgPartitionPerGroup) {
                    overLoadedGroups.add(nodeGroup);
                }
                // What about maxPartitionPerGroup ??
            }
            // distribute free partitions among under-loaded groups
            plusOneGroupCount = tryToDistributeUnownedPartitions(underLoadedGroups, freePartitions,
                    avgPartitionPerGroup, index, plusOneGroupCount);
            if (!freePartitions.isEmpty()) {
                // if there are still free partitions those could not be distributed
                // to under-loaded groups then one-by-one distribute them among all groups
                // until queue is empty.
                distributeUnownedPartitions(groups, freePartitions, index);
            }
            // TODO: what if there are still free partitions?
            // iterate through over-loaded groups' partitions and distribute them to under-loaded groups.
            transferPartitionsBetweenGroups(underLoadedGroups, overLoadedGroups, index,
                    avgPartitionPerGroup, plusOneGroupCount);
            // post process each group's partition table (distribute partitions added to group to nodes
            // and balance load of partition ownership s in group) and save partition ownerships to
            // cluster partition state table.
            updatePartitionState(state, groups, index);
        }
    }

    private void transferPartitionsBetweenGroups(Queue<NodeGroup> underLoadedGroups, Collection<NodeGroup> overLoadedGroups,
                                                 int index, int avgPartitionPerGroup, int plusOneGroupCount) {

        int maxPartitionPerGroup = avgPartitionPerGroup + 1;
        int maxTries = underLoadedGroups.size() * overLoadedGroups.size() * DEFAULT_RETRY_MULTIPLIER;
        int tries = 0;
        int expectedPartitionCount = plusOneGroupCount > 0 ? maxPartitionPerGroup : avgPartitionPerGroup;
        while (tries++ < maxTries && !underLoadedGroups.isEmpty()) {
            NodeGroup toGroup = underLoadedGroups.poll();
            Iterator<NodeGroup> overLoadedGroupsIterator = overLoadedGroups.iterator();
            while (overLoadedGroupsIterator.hasNext()) {
                NodeGroup fromGroup = overLoadedGroupsIterator.next();
                selectToGroupPartitions(index, expectedPartitionCount, toGroup, fromGroup);
                int fromCount = fromGroup.getPartitionCount(index);
                if (plusOneGroupCount > 0 && fromCount == maxPartitionPerGroup) {
                    if (--plusOneGroupCount == 0) {
                        expectedPartitionCount = avgPartitionPerGroup;
                    }
                }
                if (fromCount <= expectedPartitionCount) {
                    overLoadedGroupsIterator.remove();
                }
                int toCount = toGroup.getPartitionCount(index);
                if (plusOneGroupCount > 0 && toCount == maxPartitionPerGroup) {
                    if (--plusOneGroupCount == 0) {
                        expectedPartitionCount = avgPartitionPerGroup;
                    }
                }
                if (toCount >= expectedPartitionCount) {
                    break;
                }
            }
            if (toGroup.getPartitionCount(index) < avgPartitionPerGroup/* && !underLoadedGroups.contains(toGroup)*/) {
                underLoadedGroups.offer(toGroup);
            }
        }
    }

    private void selectToGroupPartitions(int index, int expectedPartitionCount, NodeGroup toGroup, NodeGroup fromGroup) {
        Iterator<Integer> partitionsIterator = fromGroup.getPartitionsIterator(index);
        while (partitionsIterator.hasNext()
                && fromGroup.getPartitionCount(index) > expectedPartitionCount
                && toGroup.getPartitionCount(index) < expectedPartitionCount) {
            Integer partitionId = partitionsIterator.next();
            if (toGroup.addPartition(index, partitionId)) {
                partitionsIterator.remove();
            }
        }
    }

    private void updatePartitionState(Address[][] state, Collection<NodeGroup> groups, int index) {
        for (NodeGroup group : groups) {
            group.postProcessPartitionTable(index);
            for (Address address : group.getNodes()) {
                PartitionTable table = group.getPartitionTable(address);
                Set<Integer> set = table.getPartitions(index);
                for (Integer partitionId : set) {
                    state[partitionId][index] = address;
                }
            }
        }
    }

    private void distributeUnownedPartitions(Queue<NodeGroup> groups, Queue<Integer> freePartitions, int index) {
        int groupSize = groups.size();
        int maxTries = freePartitions.size() * groupSize * DEFAULT_RETRY_MULTIPLIER;
        int tries = 0;
        Integer partitionId = freePartitions.poll();
        while (partitionId != null && tries++ < maxTries) {
            NodeGroup group = groups.poll();
            if (group.addPartition(index, partitionId)) {
                partitionId = freePartitions.poll();
            }
            groups.offer(group);
        }
    }

    private int tryToDistributeUnownedPartitions(Queue<NodeGroup> underLoadedGroups, Queue<Integer> freePartitions,
                                                 int avgPartitionPerGroup, int index, int plusOneGroupCount) {

        // distribute free partitions among under-loaded groups
        int maxPartitionPerGroup = avgPartitionPerGroup + 1;
        int maxTries = freePartitions.size() * underLoadedGroups.size();
        int tries = 0;
        while (tries++ < maxTries && !freePartitions.isEmpty() && !underLoadedGroups.isEmpty()) {
            NodeGroup group = underLoadedGroups.poll();
            assignFreePartitionsToNodeGroup(freePartitions, index, group);
            int count = group.getPartitionCount(index);
            if (plusOneGroupCount > 0 && count == maxPartitionPerGroup) {
                if (--plusOneGroupCount == 0) {
                    // all (avg + 1) partitions owned groups are found
                    // if there is any group has avg number of partitions in under-loaded queue
                    // remove it.
                    Iterator<NodeGroup> underLoaded = underLoadedGroups.iterator();
                    while (underLoaded.hasNext()) {
                        if (underLoaded.next().getPartitionCount(index) >= avgPartitionPerGroup) {
                            underLoaded.remove();
                        }
                    }
                }
            } else if ((plusOneGroupCount > 0 && count < maxPartitionPerGroup)
                    || (count < avgPartitionPerGroup)) {
                underLoadedGroups.offer(group);
            }
        }
        return plusOneGroupCount;
    }

    private void assignFreePartitionsToNodeGroup(Queue<Integer> freePartitions, int index, NodeGroup group) {
        int size = freePartitions.size();
        for (int i = 0; i < size; i++) {
            Integer partitionId = freePartitions.poll();
            if (!group.addPartition(index, partitionId)) {
                freePartitions.offer(partitionId);
            } else {
                break;
            }
        }
    }

    private Queue<Integer> getUnownedPartitions(Address[][] state, int replicaIndex) {
        LinkedList<Integer> freePartitions = new LinkedList<Integer>();
        // if owner of a partition can not be found then add partition to free partitions queue.
        for (int partitionId = 0; partitionId < state.length; partitionId++) {
            Address[] replicas = state[partitionId];
            if (replicas[replicaIndex] == null) {
                freePartitions.add(partitionId);
            }
        }
        Collections.shuffle(freePartitions);
        return freePartitions;
    }

    private void initializeGroupPartitions(Address[][] state, Queue<NodeGroup> groups, int replicaCount, boolean aggressive) {
        // reset partition before reuse
        for (NodeGroup nodeGroup : groups) {
            nodeGroup.resetPartitions();
        }
        for (int partitionId = 0; partitionId < state.length; partitionId++) {
            Address[] replicas = state[partitionId];

            for (int replicaIndex = 0; replicaIndex < InternalPartition.MAX_REPLICA_COUNT; replicaIndex++) {
                if (replicaIndex >= replicaCount) {
                    replicas[replicaIndex] = null;
                } else {
                    Address owner = replicas[replicaIndex];
                    boolean valid = false;
                    if (owner != null) {
                        valid = partitionOwnerAvailable(groups, partitionId, replicaIndex, owner);
                    }
                    if (!valid) {
                        replicas[replicaIndex] = null;
                    } else if (aggressive && replicaIndex < AGGRESSIVE_INDEX_THRESHOLD) {
                        for (int i = AGGRESSIVE_INDEX_THRESHOLD; i < replicaCount; i++) {
                            replicas[i] = null;
                        }
                    }
                }
            }
        }
    }

    private boolean partitionOwnerAvailable(Queue<NodeGroup> groups, int partitionId, int replicaIndex, Address owner) {
        for (NodeGroup nodeGroup : groups) {
            if (nodeGroup.hasNode(owner)) {
                if (nodeGroup.ownPartition(owner, replicaIndex, partitionId)) {
                    return true;
                }
                break;
            }
        }
        return false;
    }

    private Queue<NodeGroup> createNodeGroups(Collection<MemberGroup> memberGroups) {
        Queue<NodeGroup> nodeGroups = new LinkedList<NodeGroup>();
        if (memberGroups == null || memberGroups.isEmpty()) {
            return nodeGroups;
        }

        for (MemberGroup memberGroup : memberGroups) {
            NodeGroup nodeGroup;
            if (memberGroup.size() == 0) {
                continue;
            }
            if (memberGroup instanceof SingleMemberGroup || memberGroup.size() == 1) {
                nodeGroup = new SingleNodeGroup();
                MemberImpl next = (MemberImpl) memberGroup.iterator().next();
                nodeGroup.addNode(next.getAddress());
            } else {
                nodeGroup = new DefaultNodeGroup();
                Iterator<Member> iter = memberGroup.iterator();
                while (iter.hasNext()) {
                    MemberImpl next = (MemberImpl) iter.next();
                    nodeGroup.addNode(next.getAddress());
                }
            }
            nodeGroups.add(nodeGroup);
        }
        return nodeGroups;
    }

    private TestResult testArrangement(Address[][] state, Collection<NodeGroup> groups, int partitionCount) {
        float ratio = RANGE_CHECK_RATIO;
        int avgPartitionPerGroup = partitionCount / groups.size();
        int replicaCount = Math.min(groups.size(), InternalPartition.MAX_REPLICA_COUNT);
        Set<Address> set = new HashSet<Address>();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            Address[] replicas = state[partitionId];
            for (int i = 0; i < replicaCount; i++) {
                Address owner = replicas[i];
                if (owner == null) {
                    LOGGER.warning(
                            "Partition-Arrangement-Test: Owner is null !!! => partition: " + partitionId + " replica: " + i);
                    return TestResult.FAIL;
                }
                if (set.contains(owner)) {
                    // Should not happen!
                    LOGGER.warning("Partition-Arrangement-Test: " + owner + " has owned multiple replicas of partition: "
                            + partitionId + " replica: " + i);
                    return TestResult.FAIL;
                }
                set.add(owner);
            }
            set.clear();
        }
        for (NodeGroup group : groups) {
            for (int i = 0; i < replicaCount; i++) {
                int partitionCountOfGroup = group.getPartitionCount(i);
                if (Math.abs(partitionCountOfGroup - avgPartitionPerGroup) <= MIN_AVG_OWNER_DIFF) {
                    continue;
                }
                if ((partitionCountOfGroup < avgPartitionPerGroup / ratio)
                        || (partitionCountOfGroup > avgPartitionPerGroup * ratio)) {
                    if (LOGGER.isFinestEnabled()) {
                        LOGGER.finest("Replica: " + i + ", PartitionCount: "
                                + partitionCountOfGroup + ", AvgPartitionCount: " + avgPartitionPerGroup);
                    }
                    return TestResult.RETRY;
                }
            }
        }
        return TestResult.PASS;
    }
    // ----- INNER CLASSES -----

    private interface StateInitializer {
        void initialize(Address[][] state);
    }

    private static class EmptyStateInitializer implements StateInitializer {
        @Override
        public void initialize(Address[][] state) {
            for (int i = 0; i < state.length; i++) {
                state[i] = new Address[InternalPartition.MAX_REPLICA_COUNT];
            }
        }
    }

    private static class CopyStateInitializer implements StateInitializer {
        private final InternalPartition[] currentState;

        CopyStateInitializer(InternalPartition[] currentState) {
            this.currentState = currentState;
        }

        @Override
        public void initialize(Address[][] state) {
            if (state.length != currentState.length) {
                throw new IllegalArgumentException("Partition counts do not match!");
            }
            for (int partitionId = 0; partitionId < state.length; partitionId++) {
                InternalPartition p = currentState[partitionId];
                Address[] replicas = new Address[InternalPartition.MAX_REPLICA_COUNT];
                state[partitionId] = replicas;
                for (int replicaIndex = 0; replicaIndex < InternalPartition.MAX_REPLICA_COUNT; replicaIndex++) {
                    replicas[replicaIndex] = p.getReplicaAddress(replicaIndex);
                }
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

        int getPartitionCount(int index);

        boolean containsPartition(Integer partitionId);

        boolean ownPartition(Address address, int index, Integer partitionId);

        boolean addPartition(int replicaIndex, Integer partitionId);

        Iterator<Integer> getPartitionsIterator(int index);

        boolean removePartition(int index, Integer partitionId);

        void postProcessPartitionTable(int index);
    }

    private static class DefaultNodeGroup implements NodeGroup {
        final PartitionTable groupPartitionTable = new PartitionTable();
        final Map<Address, PartitionTable> nodePartitionTables = new HashMap<Address, PartitionTable>();
        final Set<Address> nodes = nodePartitionTables.keySet();
        final Collection<PartitionTable> nodeTables = nodePartitionTables.values();
        final LinkedList<Integer> partitionQ = new LinkedList<Integer>();

        @Override
        public void addNode(Address address) {
            nodePartitionTables.put(address, new PartitionTable());
        }

        @Override
        public boolean hasNode(Address address) {
            return nodes.contains(address);
        }

        @Override
        public Set<Address> getNodes() {
            return nodes;
        }

        @Override
        public PartitionTable getPartitionTable(Address address) {
            return nodePartitionTables.get(address);
        }

        @Override
        public void resetPartitions() {
            groupPartitionTable.reset();
            partitionQ.clear();
            for (PartitionTable table : nodeTables) {
                table.reset();
            }
        }

        @Override
        public int getPartitionCount(int index) {
            return groupPartitionTable.size(index);
        }

        @Override
        public boolean containsPartition(Integer partitionId) {
            return groupPartitionTable.contains(partitionId);
        }

        @Override
        public boolean ownPartition(Address address, int index, Integer partitionId) {
            if (!hasNode(address)) {
                String error = "Address does not belong to this group: " + address.toString();
                LOGGER.warning(error);
                return false;
            }
            if (containsPartition(partitionId)) {
                if (LOGGER.isFinestEnabled()) {
                    String error = "Partition[" + partitionId + "] is already owned by this group! " + "Duplicate!";
                    LOGGER.finest(error);
                }
                return false;
            }
            groupPartitionTable.add(index, partitionId);
            return nodePartitionTables.get(address).add(index, partitionId);
        }

        @Override
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

        @Override
        public Iterator<Integer> getPartitionsIterator(final int index) {
            final Iterator<Integer> iterator = groupPartitionTable.getPartitions(index).iterator();
            return new Iterator<Integer>() {
                Integer current;

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Integer next() {
                    current = iterator.next();
                    return current;
                }

                @Override
                public void remove() {
                    iterator.remove();
                    doRemovePartition(index, current);
                }
            };
        }

        @Override
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

        @Override
        public void postProcessPartitionTable(int index) {
            if (nodes.size() == 1) {
                PartitionTable table = nodeTables.iterator().next();
                while (!partitionQ.isEmpty()) {
                    table.add(index, partitionQ.poll());
                }
            } else {
                List<PartitionTable> underLoadedStates = new LinkedList<PartitionTable>();
                int avgCount = slimDownNodesToAvgPartitionTableSize(index, underLoadedStates);
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

        private int slimDownNodesToAvgPartitionTableSize(int index, List<PartitionTable> underLoadedStates) {
            int totalCount = getPartitionCount(index);
            int avgCount = totalCount / nodes.size();
            for (PartitionTable table : nodeTables) {
                Set<Integer> partitions = table.getPartitions(index);
                if (partitions.size() > avgCount) {
                    Integer[] partitionArray = partitions.toArray(new Integer[partitions.size()]);
                    while (partitions.size() > avgCount) {
                        int partitionId = partitionArray[partitions.size() - 1];
                        partitions.remove(partitionId);
                        partitionQ.add(partitionId);
                    }
                } else {
                    underLoadedStates.add(table);
                }
            }
            return avgCount;
        }

        @Override
        public String toString() {
            return "DefaultNodeGroupRegistry [nodes=" + nodes + "]";
        }
    }

    private static class SingleNodeGroup implements NodeGroup {
        final PartitionTable nodeTable = new PartitionTable();
        Address address;
        Set<Address> nodes;

        @Override
        public void addNode(Address addr) {
            if (address != null) {
                LOGGER.warning("Single node group already has an address => " + address);
                return;
            }
            this.address = addr;
            nodes = Collections.singleton(address);
        }

        @Override
        public boolean hasNode(Address address) {
            return this.address != null && this.address.equals(address);
        }

        @Override
        public Set<Address> getNodes() {
            return nodes;
        }

        @Override
        public PartitionTable getPartitionTable(Address address) {
            return hasNode(address) ? nodeTable : null;
        }

        @Override
        public void resetPartitions() {
            nodeTable.reset();
        }

        @Override
        public int getPartitionCount(int index) {
            return nodeTable.size(index);
        }

        @Override
        public boolean containsPartition(Integer partitionId) {
            return nodeTable.contains(partitionId);
        }

        @Override
        public boolean ownPartition(Address address, int index, Integer partitionId) {
            if (!hasNode(address)) {
                String error = address + " is different from this node's " + this.address;
                LOGGER.warning(error);
                return false;
            }
            if (containsPartition(partitionId)) {
                if (LOGGER.isFinestEnabled()) {
                    String error = "Partition[" + partitionId + "] is already owned by this node " + address + "! Duplicate!";
                    LOGGER.finest(error);
                }
                return false;
            }
            return nodeTable.add(index, partitionId);
        }

        @Override
        public boolean addPartition(int replicaIndex, Integer partitionId) {
            if (containsPartition(partitionId)) {
                return false;
            }
            return nodeTable.add(replicaIndex, partitionId);
        }

        @Override
        public Iterator<Integer> getPartitionsIterator(int index) {
            return nodeTable.getPartitions(index).iterator();
        }

        @Override
        public boolean removePartition(int index, Integer partitionId) {
            return nodeTable.remove(index, partitionId);
        }

        @Override
        public void postProcessPartitionTable(int index) {
        }

        @Override
        public String toString() {
            return "SingleNodeGroupRegistry [address=" + address + "]";
        }
    }

    @SuppressWarnings("unchecked")
    private static class PartitionTable {
        final Set<Integer>[] partitions = new Set[InternalPartition.MAX_REPLICA_COUNT];

        Set<Integer> getPartitions(int index) {
            check(index);
            Set<Integer> set = partitions[index];
            if (set == null) {
                set = new LinkedHashSet<Integer>();
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
            for (Set<Integer> set : partitions) {
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
            for (Set<Integer> set : partitions) {
                if (set != null) {
                    set.clear();
                }
            }
        }

        private void check(int index) {
            if (index < 0 || index >= InternalPartition.MAX_REPLICA_COUNT) {
                throw new ArrayIndexOutOfBoundsException(index);
            }
        }
    }
}
