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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionStateGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.partitiongroup.MemberGroup;
import com.hazelcast.internal.partition.membergroup.SingleMemberGroup;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

final class PartitionStateGeneratorImpl implements PartitionStateGenerator {

    private static final ILogger LOGGER = Logger.getLogger(PartitionStateGenerator.class);

    private static final int DEFAULT_RETRY_MULTIPLIER = 10;

    private static final float RANGE_CHECK_RATIO = 1.1f;
    private static final int MAX_RETRY_COUNT = 3;
    private static final int AGGRESSIVE_RETRY_THRESHOLD = 1;
    private static final int AGGRESSIVE_INDEX_THRESHOLD = 3;
    private static final int MIN_AVG_OWNER_DIFF = 3;

    @Override
    public PartitionReplica[][] arrange(Collection<MemberGroup> memberGroups, InternalPartition[] currentState) {
        return arrange(memberGroups, currentState, null);
    }

    @Override
    public PartitionReplica[][] arrange(Collection<MemberGroup> memberGroups, InternalPartition[] currentState,
            Collection<Integer> partitions) {


        Queue<NodeGroup> groups = createNodeGroups(memberGroups);
        if (groups.isEmpty()) {
            return null;
        }

        int partitionCount = currentState.length;
        PartitionReplica[][] state = new PartitionReplica[partitionCount][InternalPartition.MAX_REPLICA_COUNT];
        initialize(currentState, state, partitions);

        int tryCount = 0;
        do {
            boolean aggressive = tryCount >= AGGRESSIVE_RETRY_THRESHOLD;
            tryArrange(state, groups, partitionCount, aggressive, partitions);
            if (tryCount++ > 0) {
                if (LOGGER.isFineEnabled()) {
                    LOGGER.fine("Re-trying partition arrangement. Count: " + tryCount);
                }
            }
        } while (tryCount < MAX_RETRY_COUNT && !areGroupsBalanced(groups, partitionCount));

        return state;
    }

    private void initialize(InternalPartition[] currentState, PartitionReplica[][] state, Collection<Integer> partitions) {
        int partitionCount = currentState.length;

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartition p = currentState[partitionId];
            PartitionReplica[] replicas = state[partitionId];
            boolean empty = true;

            for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                replicas[index] = p.getReplica(index);
                empty &= replicas[index] == null;
            }

            if (empty || partitions != null && !partitions.contains(partitionId)) {
                continue;
            }

            // auto shift-up colder replicas to hotter replicas to fill the empty gaps
            int maxReplicaIndex = InternalPartition.MAX_REPLICA_COUNT - 1;
            for (int index = 0; index < InternalPartition.MAX_REPLICA_COUNT; index++) {
                if (replicas[index] == null) {
                    for (int k = maxReplicaIndex; k > index; k--) {
                        if (replicas[k] != null) {
                            replicas[index] = replicas[k];
                            replicas[k] = null;
                            maxReplicaIndex = k - 1;
                            break;
                        }
                    }
                }
            }
        }
    }

    private void tryArrange(PartitionReplica[][] state, Queue<NodeGroup> groups, int partitionCount, boolean aggressive,
            Collection<Integer> toBeArrangedPartitions) {

        int groupSize = groups.size();
        int replicaCount = Math.min(groupSize, InternalPartition.MAX_REPLICA_COUNT);
        int avgPartitionPerGroup = partitionCount / groupSize;

        // clear unused replica owners
        // initialize partition registry for each group
        initializeGroupPartitions(state, groups, replicaCount, aggressive, toBeArrangedPartitions);

        for (int index = 0; index < replicaCount; index++) {
            // partitions those are not bound to any node/group
            Queue<Integer> freePartitions = getUnownedPartitions(state, index);

            // retain only to-be-arranged partitions
            if (toBeArrangedPartitions != null) {
                freePartitions.retainAll(toBeArrangedPartitions);
            }

            // groups having partitions under average
            Queue<NodeGroup> underLoadedGroups = new LinkedList<>();
            // groups having partitions over average
            List<NodeGroup> overLoadedGroups = new LinkedList<>();
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
            assert freePartitions.isEmpty() : "There are partitions not-owned yet: " + freePartitions;

            if (toBeArrangedPartitions == null) {
                // iterate through over-loaded groups' partitions and distribute them to under-loaded groups.
                transferPartitionsBetweenGroups(underLoadedGroups, overLoadedGroups, index, avgPartitionPerGroup,
                        plusOneGroupCount);
            }
            // post process each group's partition table (distribute partitions added to group to nodes
            // and balance load of partition ownership s in group) and save partition ownerships to
            // cluster partition state table.
            updatePartitionState(state, groups, index);
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
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

    private void updatePartitionState(PartitionReplica[][] state, Collection<NodeGroup> groups, int index) {
        for (NodeGroup group : groups) {
            group.postProcessPartitionTable(index);
            for (PartitionReplica replica : group.getReplicas()) {
                PartitionTable table = group.getPartitionTable(replica);
                Set<Integer> set = table.getPartitions(index);
                for (Integer partitionId : set) {
                    state[partitionId][index] = replica;
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
                    underLoadedGroups.removeIf(nodeGroup -> nodeGroup.getPartitionCount(index) >= avgPartitionPerGroup);
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

    private Queue<Integer> getUnownedPartitions(PartitionReplica[][] state, int replicaIndex) {
        LinkedList<Integer> freePartitions = new LinkedList<>();
        // if owner of a partition can not be found then add partition to free partitions queue.
        for (int partitionId = 0; partitionId < state.length; partitionId++) {
            PartitionReplica[] replicas = state[partitionId];
            if (replicas[replicaIndex] == null) {
                freePartitions.add(partitionId);
            }
        }
        Collections.shuffle(freePartitions);
        return freePartitions;
    }

    private void initializeGroupPartitions(PartitionReplica[][] state, Queue<NodeGroup> groups, int replicaCount,
            boolean aggressive, Collection<Integer> toBeArrangedPartitions) {
        // reset partition before reuse
        for (NodeGroup nodeGroup : groups) {
            nodeGroup.resetPartitions();
        }
        for (int partitionId = 0; partitionId < state.length; partitionId++) {
            PartitionReplica[] replicas = state[partitionId];

            for (int replicaIndex = 0; replicaIndex < InternalPartition.MAX_REPLICA_COUNT; replicaIndex++) {
                if (replicaIndex >= replicaCount) {
                    replicas[replicaIndex] = null;
                    continue;
                }

                PartitionReplica owner = replicas[replicaIndex];
                boolean valid = false;
                if (owner != null) {
                    valid = partitionOwnerAvailable(groups, partitionId, replicaIndex, owner);
                }
                if (!valid) {
                    replicas[replicaIndex] = null;
                } else if (aggressive && replicaIndex < AGGRESSIVE_INDEX_THRESHOLD
                        && (toBeArrangedPartitions == null || toBeArrangedPartitions.contains(partitionId))) {
                    for (int i = AGGRESSIVE_INDEX_THRESHOLD; i < replicaCount; i++) {
                        replicas[i] = null;
                    }
                }
            }
        }
    }

    private boolean partitionOwnerAvailable(Queue<NodeGroup> groups, int partitionId, int replicaIndex, PartitionReplica owner) {
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
        Queue<NodeGroup> nodeGroups = new LinkedList<>();
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
                Member next = memberGroup.iterator().next();
                nodeGroup.addNode(PartitionReplica.from(next));
            } else {
                nodeGroup = new DefaultNodeGroup();
                Iterator<Member> iter = memberGroup.iterator();
                while (iter.hasNext()) {
                    Member next = iter.next();
                    nodeGroup.addNode(PartitionReplica.from(next));
                }
            }
            nodeGroups.add(nodeGroup);
        }
        return nodeGroups;
    }

    private boolean areGroupsBalanced(Collection<NodeGroup> groups, int partitionCount) {
        float ratio = RANGE_CHECK_RATIO;
        int avgPartitionPerGroup = partitionCount / groups.size();
        int replicaCount = Math.min(groups.size(), InternalPartition.MAX_REPLICA_COUNT);

        for (NodeGroup group : groups) {
            for (int i = 0; i < replicaCount; i++) {
                int partitionCountOfGroup = group.getPartitionCount(i);
                if (Math.abs(partitionCountOfGroup - avgPartitionPerGroup) <= MIN_AVG_OWNER_DIFF) {
                    continue;
                }
                if ((partitionCountOfGroup < avgPartitionPerGroup / ratio)
                        || (partitionCountOfGroup > avgPartitionPerGroup * ratio)) {
                    if (LOGGER.isFineEnabled()) {
                        LOGGER.fine("Not well balanced! Replica: " + i + ", PartitionCount: "
                                + partitionCountOfGroup + ", AvgPartitionCount: " + avgPartitionPerGroup);
                    }
                    return false;
                }
            }
        }
        return true;
    }
    // ----- INNER CLASSES -----

    private interface NodeGroup {
        void addNode(PartitionReplica replica);

        boolean hasNode(PartitionReplica replica);

        Set<PartitionReplica> getReplicas();

        PartitionTable getPartitionTable(PartitionReplica replica);

        void resetPartitions();

        int getPartitionCount(int index);

        boolean ownPartition(PartitionReplica replica, int index, Integer partitionId);

        boolean addPartition(int replicaIndex, Integer partitionId);

        Iterator<Integer> getPartitionsIterator(int index);

        void postProcessPartitionTable(int index);
    }

    private static class DefaultNodeGroup implements NodeGroup {
        final PartitionTable groupPartitionTable = new PartitionTable();
        final Map<PartitionReplica, PartitionTable> nodePartitionTables = new HashMap<>();
        final LinkedList<Integer> partitionQ = new LinkedList<>();

        @Override
        public void addNode(PartitionReplica replica) {
            nodePartitionTables.put(replica, new PartitionTable());
        }

        @Override
        public boolean hasNode(PartitionReplica replica) {
            return nodePartitionTables.containsKey(replica);
        }

        @Override
        public Set<PartitionReplica> getReplicas() {
            return nodePartitionTables.keySet();
        }

        @Override
        public PartitionTable getPartitionTable(PartitionReplica replica) {
            return nodePartitionTables.get(replica);
        }

        @Override
        public void resetPartitions() {
            groupPartitionTable.reset();
            partitionQ.clear();
            for (PartitionTable table : nodePartitionTables.values()) {
                table.reset();
            }
        }

        @Override
        public int getPartitionCount(int index) {
            return groupPartitionTable.size(index);
        }

        private boolean containsPartition(Integer partitionId) {
            return groupPartitionTable.contains(partitionId);
        }

        @Override
        public boolean ownPartition(PartitionReplica replica, int index, Integer partitionId) {
            if (!hasNode(replica)) {
                String error = "PartitionReplica does not belong to this group: " + replica.toString();
                LOGGER.warning(error);
                return false;
            }
            if (containsPartition(partitionId)) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Partition[" + partitionId + "] is already owned by this group!");
                }
                return false;
            }
            groupPartitionTable.add(index, partitionId);
            return nodePartitionTables.get(replica).add(index, partitionId);
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

        private void doRemovePartition(int index, Integer partitionId) {
            for (PartitionTable table : nodePartitionTables.values()) {
                if (table.remove(index, partitionId)) {
                    break;
                }
            }
        }

        @Override
        public void postProcessPartitionTable(int index) {
            if (nodePartitionTables.size() == 1) {
                PartitionTable table = nodePartitionTables.values().iterator().next();
                while (!partitionQ.isEmpty()) {
                    table.add(index, partitionQ.poll());
                }
            } else {
                List<PartitionTable> underLoadedStates = new LinkedList<>();
                int avgCount = slimDownNodesToAvgPartitionTableSize(index, underLoadedStates);
                if (!partitionQ.isEmpty()) {
                    for (PartitionTable table : underLoadedStates) {
                        while (table.size(index) < avgCount) {
                            table.add(index, partitionQ.poll());
                        }
                    }
                }
                while (!partitionQ.isEmpty()) {
                    for (PartitionTable table : nodePartitionTables.values()) {
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
            int avgCount = totalCount / nodePartitionTables.values().size();
            for (PartitionTable table : nodePartitionTables.values()) {
                Set<Integer> partitions = table.getPartitions(index);
                if (partitions.size() > avgCount) {
                    Integer[] partitionArray = partitions.toArray(new Integer[0]);
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
            return "DefaultNodeGroupRegistry [nodes=" + nodePartitionTables.keySet() + "]";
        }
    }

    private static class SingleNodeGroup implements NodeGroup {
        final PartitionTable nodeTable = new PartitionTable();
        PartitionReplica replica;
        Set<PartitionReplica> replicas;

        @Override
        public void addNode(PartitionReplica replica) {
            if (this.replica != null) {
                LOGGER.warning("Single node group already has an address => " + this.replica);
                return;
            }
            this.replica = replica;
            replicas = Collections.singleton(replica);
        }

        @Override
        public boolean hasNode(PartitionReplica replica) {
            return this.replica != null && this.replica.equals(replica);
        }

        @Override
        public Set<PartitionReplica> getReplicas() {
            return replicas;
        }

        @Override
        public PartitionTable getPartitionTable(PartitionReplica replica) {
            return hasNode(replica) ? nodeTable : null;
        }

        @Override
        public void resetPartitions() {
            nodeTable.reset();
        }

        @Override
        public int getPartitionCount(int index) {
            return nodeTable.size(index);
        }

        private boolean containsPartition(Integer partitionId) {
            return nodeTable.contains(partitionId);
        }

        @Override
        public boolean ownPartition(PartitionReplica replica, int index, Integer partitionId) {
            if (!hasNode(replica)) {
                String error = replica + " is different from this node's " + this.replica;
                LOGGER.warning(error);
                return false;
            }
            if (containsPartition(partitionId)) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Partition[" + partitionId + "] is already owned by this node " + replica);
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
        public void postProcessPartitionTable(int index) {
        }

        @Override
        public String toString() {
            return "SingleNodeGroupRegistry [address=" + replica + "]";
        }
    }

    @SuppressWarnings("unchecked")
    private static class PartitionTable {
        final Set<Integer>[] partitions = new Set[InternalPartition.MAX_REPLICA_COUNT];

        Set<Integer> getPartitions(int index) {
            check(index);
            Set<Integer> set = partitions[index];
            if (set == null) {
                set = new LinkedHashSet<>();
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
