/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionListener;
import com.hazelcast.internal.partition.PartitionStateGenerator;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.membergroup.MemberGroup;
import com.hazelcast.partition.membergroup.MemberGroupFactory;
import com.hazelcast.partition.membergroup.MemberGroupFactoryFactory;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * Maintains the partition table state.
 */
public class PartitionStateManager {

    private final Node node;
    private final ILogger logger;
    private final InternalPartitionServiceImpl partitionService;

    private final int partitionCount;
    private final InternalPartitionImpl[] partitions;

    @Probe
    private final AtomicInteger stateVersion = new AtomicInteger();

    private final PartitionStateGenerator partitionStateGenerator;
    private final MemberGroupFactory memberGroupFactory;

    // updates will be done under lock, but reads will be multithreaded.
    // set to true when the partitions are assigned for the first time. remains true until partition service has been reset.
    private volatile boolean initialized;

    @Probe
    // can be read and written concurrently...
    private volatile int memberGroupsSize;

    public PartitionStateManager(Node node, InternalPartitionServiceImpl partitionService, PartitionListener listener) {
        this.node = node;
        this.logger = node.getLogger(getClass());

        this.partitionService = partitionService;
        partitionCount = partitionService.getPartitionCount();
        this.partitions = new InternalPartitionImpl[partitionCount];

        Address thisAddress = node.getThisAddress();
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new InternalPartitionImpl(i, listener, thisAddress);
        }

        memberGroupFactory = MemberGroupFactoryFactory.newMemberGroupFactory(node.getConfig().getPartitionGroupConfig(),
                node.getDiscoveryService());
        partitionStateGenerator = new PartitionStateGeneratorImpl();
    }

    @Probe
    private int localPartitionCount() {
        int count = 0;
        for (InternalPartition partition : partitions) {
            if (partition.isLocal()) {
                count++;
            }
        }
        return count;
    }

    private Collection<MemberGroup> createMemberGroups(final Set<Address> excludedAddresses) {
        MemberSelector exclude = new MemberSelector() {
            @Override
            public boolean select(Member member) {
                return !excludedAddresses.contains(member.getAddress());
            }
        };
        final MemberSelector selector = MemberSelectors.and(DATA_MEMBER_SELECTOR, exclude);
        final Collection<Member> members = node.getClusterService().getMembers(selector);
        return memberGroupFactory.createMemberGroups(members);
    }

    private Collection<MemberGroup> createMemberGroups() {
        final Collection<Member> members = node.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        return memberGroupFactory.createMemberGroups(members);
    }

    /**
     * Arranges the partitions if:
     * <ul>
     * <li>this instance {@link NodeExtension#isStartCompleted()}</li>
     * <li>the cluster state allows migrations. See {@link ClusterState#isMigrationAllowed()}</li>
     * </ul>
     * This will also set the manager state to initialized (if not already) and invoke the
     * {@link PartitionListener#replicaChanged(PartitionReplicaChangeEvent)} for all changed replicas which
     * will cancel replica synchronizations and increase the partition state version.
     *
     * @param excludedAddresses members which are to be excluded from the new layout
     * @return if the new partition was assigned
     * @throws HazelcastException if the partition state generator failed to arrange the partitions
     */
    boolean initializePartitionAssignments(Set<Address> excludedAddresses) {
        if (!isPartitionAssignmentAllowed()) {
            return false;
        }

        Collection<MemberGroup> memberGroups = createMemberGroups(excludedAddresses);
        if (memberGroups.isEmpty()) {
            logger.warning("No member group is available to assign partition ownership...");
            return false;
        }

        logger.info("Initializing cluster partition table arrangement...");
        Address[][] newState = partitionStateGenerator.arrange(memberGroups, partitions);
        if (newState.length != partitionCount) {
            throw new HazelcastException("Invalid partition count! "
                    + "Expected: " + partitionCount + ", Actual: " + newState.length);
        }

        // increment state version to make fail cluster state transaction
        // if it's started and not locked the state yet.
        stateVersion.incrementAndGet();
        ClusterState clusterState = node.getClusterService().getClusterState();
        if (!clusterState.isMigrationAllowed()) {
            // cluster state is either changed or locked, decrement version back and fail.
            stateVersion.decrementAndGet();
            logger.warning("Partitions can't be assigned since cluster-state= " + clusterState);
            return false;
        }

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartitionImpl partition = partitions[partitionId];
            Address[] replicas = newState[partitionId];
            partition.setReplicaAddresses(replicas);
        }
        setInitialized();
        return true;
    }

    /**
     * Returns {@code true} if the node has started and
     * the cluster state allows migrations (see {@link ClusterState#isMigrationAllowed()}).
     * */
    private boolean isPartitionAssignmentAllowed() {
        if (!node.getNodeExtension().isStartCompleted()) {
            logger.warning("Partitions can't be assigned since startup is not completed yet.");
            return false;
        }

        ClusterState clusterState = node.getClusterService().getClusterState();
        if (!clusterState.isMigrationAllowed()) {
            logger.warning("Partitions can't be assigned since cluster-state= " + clusterState);
            return false;
        }
        if (partitionService.isFetchMostRecentPartitionTableTaskRequired()) {
            logger.warning("Partitions can't be assigned since most recent partition table is not decided yet.");
            return false;
        }
        return true;
    }

    /**
     * Sets the initial partition table and state version. If any partition has a replica, the partition state manager is
     * set to initialized, otherwise {@link #isInitialized()} stays uninitialized but the current state will be updated
     * nevertheless.
     *
     * @param partitionTable the initial partition table
     * @throws IllegalStateException if the partition manager has already been initialized
     */
    void setInitialState(PartitionTableView partitionTable) {
        if (initialized) {
            throw new IllegalStateException("Partition table is already initialized!");
        }
        logger.info("Setting cluster partition table...");
        boolean foundReplica = false;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartitionImpl partition = partitions[partitionId];
            Address[] replicas = partitionTable.getAddresses(partitionId);
            if (!foundReplica && replicas != null) {
                for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                    foundReplica |= replicas[i] != null;
                }
            }
            partition.setInitialReplicaAddresses(replicas);
        }
        stateVersion.set(partitionTable.getVersion());
        if (foundReplica) {
            setInitialized();
        }
    }

    void updateMemberGroupsSize() {
        final Collection<MemberGroup> groups = createMemberGroups();
        int size = 0;
        for (MemberGroup group : groups) {
            if (group.size() > 0) {
                size++;
            }
        }
        memberGroupsSize = size;
    }

    int getMemberGroupsSize() {
        int size = memberGroupsSize;
        if (size > 0) {
            return size;
        }

        // size = 0 means service is not initialized yet.
        // return 1 if current node is a data member since there should be at least one member group
        return node.isLiteMember() ? 0 : 1;
    }

    /**
     * Checks all replicas for all partitions. If the cluster service does not contain the member for any
     * address in the partition table, it will remove the address from the partition.
     *
     * @see ClusterService#getMember(Address)
     */
    void removeUnknownAddresses() {
        ClusterServiceImpl clusterService = node.getClusterService();

        for (InternalPartitionImpl partition : partitions) {
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                Address address = partition.getReplicaAddress(i);
                if (address == null) {
                    continue;
                }

                MemberImpl member = clusterService.getMember(address);
                if (member == null) {
                    partition.setReplicaAddress(i, null);
                    if (logger.isFinestEnabled()) {
                        logger.finest("PartitionId=" + partition.getPartitionId() + " " + address
                                + " is removed from replica index: " + i + ", partition: " + partition);
                    }
                }
            }
        }
    }

    boolean isAbsentInPartitionTable(Address address) {
        for (InternalPartitionImpl partition : partitions) {
            if (partition.isOwnerOrBackup(address)) {
                return false;
            }
        }
        return true;
    }

    boolean isPresentInPartitionTable(Address address) {
        return !isAbsentInPartitionTable(address);
    }

    InternalPartition[] getPartitions() {
        return partitions;
    }

    /** Returns a copy of the current partition table. */
    public InternalPartition[] getPartitionsCopy() {
        NopPartitionListener listener = new NopPartitionListener();
        InternalPartition[] result = new InternalPartition[partitions.length];
        for (int i = 0; i < partitionCount; i++) {
            result[i] = partitions[i].copy(listener);
        }
        return result;
    }

    public InternalPartitionImpl getPartitionImpl(int partitionId) {
        return partitions[partitionId];
    }

    Address[][] repartition(Set<Address> excludedAddresses, Collection<Integer> partitionInclusionSet) {
        if (!initialized) {
            return null;
        }
        Collection<MemberGroup> memberGroups = createMemberGroups(excludedAddresses);
        Address[][] newState = partitionStateGenerator.arrange(memberGroups, partitions, partitionInclusionSet);

        if (newState == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Partition rearrangement failed. Number of member groups: " + memberGroups.size());
            }
        }

        return newState;
    }

    public void setMigratingFlag(int partitionId) {
        if (logger.isFinestEnabled()) {
            logger.finest("Setting partition-migrating flag. partitionId=" + partitionId);
        }
        partitions[partitionId].setMigrating(true);
    }

    public void clearMigratingFlag(int partitionId) {
        if (logger.isFinestEnabled()) {
            logger.finest("Clearing partition-migrating flag. partitionId=" + partitionId);
        }
        partitions[partitionId].setMigrating(false);
    }

    /** Sets the replica addresses for the {@code partitionId}. */
    void updateReplicaAddresses(int partitionId, Address[] replicaAddresses) {
        InternalPartitionImpl partition = partitions[partitionId];
        partition.setReplicaAddresses(replicaAddresses);
    }

    // called under partition service lock
    void setVersion(int version) {
        stateVersion.set(version);
    }

    public int getVersion() {
        return stateVersion.get();
    }

    void incrementVersion(int delta) {
        if (delta >= 0) {
            stateVersion.addAndGet(delta);
        } else {
            logger.warning("partition table version not incremented by " + delta);
        }
    }

    public void incrementVersion() {
        stateVersion.incrementAndGet();
    }

    boolean setInitialized() {
        if (!initialized) {
            initialized = true;
            node.getNodeExtension().onPartitionStateChange();
            return true;
        }
        return false;
    }

    public boolean isInitialized() {
        return initialized;
    }

    void reset() {
        initialized = false;
        stateVersion.set(0);
        for (InternalPartitionImpl partition : partitions) {
            partition.reset();
        }
    }

    int replaceAddress(Address oldAddress, Address newAddress) {
        if (!initialized) {
            return 0;
        }
        int count = 0;
        for (InternalPartitionImpl partition : partitions) {
            if (partition.replaceAddress(oldAddress, newAddress) > -1) {
                count++;
            }
        }
        if (count > 0) {
            node.getNodeExtension().onPartitionStateChange();
            logger.info("Replaced " + oldAddress + " with " + newAddress + " in partition table in "
                    + count + " partitions.");
        }
        return count;
    }

    PartitionTableView getPartitionTable() {
        if (!initialized) {
            return new PartitionTableView(new Address[partitions.length][InternalPartition.MAX_REPLICA_COUNT], 0);
        }
        return new PartitionTableView(partitions, stateVersion.get());
    }
}
