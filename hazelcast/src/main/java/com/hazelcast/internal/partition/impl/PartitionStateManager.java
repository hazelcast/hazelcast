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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaInterceptor;
import com.hazelcast.internal.partition.PartitionStateGenerator;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.partition.ReadonlyInternalPartition;
import com.hazelcast.internal.partition.membergroup.MemberGroupFactory;
import com.hazelcast.internal.partition.membergroup.MemberGroupFactoryFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.partitiongroup.MemberGroup;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_ACTIVE_PARTITION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_LOCAL_PARTITION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_MEMBER_GROUP_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_PARTITION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_STAMP;
import static com.hazelcast.internal.partition.PartitionStampUtil.calculateStamp;

/**
 * Maintains the partition table state.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class PartitionStateManager {

    /**
     * Initial value of the partition table stamp.
     * If stamp has this initial value then that means
     * partition table is not initialized yet.
     */
    static final long INITIAL_STAMP = 0L;

    private final Node node;
    private final ILogger logger;
    private final InternalPartitionServiceImpl partitionService;

    @Probe(name = PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_PARTITION_COUNT)
    private final int partitionCount;
    private final InternalPartitionImpl[] partitions;

    private final PartitionStateGenerator partitionStateGenerator;
    private final MemberGroupFactory memberGroupFactory;

    // snapshot of partition assignments taken on member UUID removal and
    // before partition rebalancing
    private final ConcurrentMap<UUID, PartitionTableView> snapshotOnRemove;

    // we keep a cached buffer because stamp calculation can happen many times
    // during repartitioning and this introduces GC pressure, especially at high
    // partition counts
    private final byte[] stampCalculationBuffer;

    // updates will be done under lock, but reads will be multithreaded.
    // set to true when the partitions are assigned for the first time. remains true until partition service has been reset.
    private volatile boolean initialized;

    @Probe(name = PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_STAMP)
    // can be read and written concurrently...
    private volatile long stateStamp = INITIAL_STAMP;

    @Probe(name = PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_MEMBER_GROUP_SIZE)
    // can be read and written concurrently...
    private volatile int memberGroupsSize;

    public PartitionStateManager(Node node, InternalPartitionServiceImpl partitionService) {
        this.node = node;
        this.logger = node.getLogger(getClass());

        this.partitionService = partitionService;
        this.partitionCount = partitionService.getPartitionCount();
        this.partitions = new InternalPartitionImpl[partitionCount];
        this.stampCalculationBuffer = new byte[partitionCount * Integer.BYTES];

        PartitionReplicaInterceptor interceptor = new DefaultPartitionReplicaInterceptor(partitionService);
        PartitionReplica localReplica = PartitionReplica.from(node.getLocalMember());
        for (int i = 0; i < partitionCount; i++) {
            this.partitions[i] = new InternalPartitionImpl(i, localReplica, interceptor);
        }

        memberGroupFactory = MemberGroupFactoryFactory.newMemberGroupFactory(node.getConfig().getPartitionGroupConfig(),
                node.getDiscoveryService());
        partitionStateGenerator = new PartitionStateGeneratorImpl();
        snapshotOnRemove = new ConcurrentHashMap<>();
    }

    /**
     * @return {@code true} if there are partitions having {@link
     * InternalPartitionImpl#isMigrating()} flag set, {@code false} otherwise.
     */
    boolean hasMigratingPartitions() {
        for (int i = 0; i < partitionCount; ++i) {
            if (partitions[i].isMigrating()) {
                return true;
            }
        }
        return false;
    }

    @Probe(name = PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_LOCAL_PARTITION_COUNT)
    private int localPartitionCount() {
        int count = 0;
        for (InternalPartition partition : partitions) {
            if (partition.isLocal()) {
                count++;
            }
        }
        return count;
    }

    @Probe(name = PARTITIONS_METRIC_PARTITION_REPLICA_STATE_MANAGER_ACTIVE_PARTITION_COUNT)
    private int activePartitionCount() {
        return partitionService.getMemberPartitionsIfAssigned(node.getThisAddress()).size();
    }

    private Collection<MemberGroup> createMemberGroups(final Set<Member> excludedMembers) {
        MemberSelector exclude = member -> !excludedMembers.contains(member);
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
     * {@link DefaultPartitionReplicaInterceptor} for all changed replicas which
     * will cancel replica synchronizations and increase the partition state version.
     *
     * @param excludedMembers members which are to be excluded from the new layout
     * @return if the new partition was assigned
     * @throws HazelcastException if the partition state generator failed to arrange the partitions
     */
    boolean initializePartitionAssignments(Set<Member> excludedMembers) {
        if (!isPartitionAssignmentAllowed()) {
            return false;
        }

        Collection<MemberGroup> memberGroups = createMemberGroups(excludedMembers);
        if (memberGroups.isEmpty()) {
            logger.warning("No member group is available to assign partition ownership...");
            return false;
        }

        logger.info("Initializing cluster partition table arrangement...");
        PartitionReplica[][] newState = partitionStateGenerator.arrange(memberGroups, partitions);
        if (newState.length != partitionCount) {
            throw new HazelcastException("Invalid partition count! "
                    + "Expected: " + partitionCount + ", Actual: " + newState.length);
        }

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartitionImpl partition = partitions[partitionId];
            PartitionReplica[] replicas = newState[partitionId];
            partition.setReplicas(replicas);
        }

        ClusterState clusterState = node.getClusterService().getClusterState();
        if (!clusterState.isMigrationAllowed()) {
            // cluster state is either changed or locked, reset state back and fail.
            reset();
            logger.warning("Partitions can't be assigned since cluster-state= " + clusterState);
            return false;
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
        PartitionReplica localReplica = PartitionReplica.from(node.getLocalMember());
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartitionImpl partition = partitions[partitionId];
            InternalPartition newPartition = partitionTable.getPartition(partitionId);
            if (!foundReplica && newPartition != null) {
                for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                    foundReplica |= newPartition.getReplica(i) != null;
                }
            }
            partition.reset(localReplica);
            if (newPartition != null) {
                partition.setReplicasAndVersion(newPartition);
            }
        }
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
     * @see ClusterService#getMember(Address, UUID)
     */
    void removeUnknownMembers() {
        ClusterServiceImpl clusterService = node.getClusterService();

        for (InternalPartitionImpl partition : partitions) {
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                PartitionReplica replica = partition.getReplica(i);
                if (replica == null) {
                    continue;
                }

                if (clusterService.getMember(replica.address(), replica.uuid()) == null) {
                    partition.setReplica(i, null);
                    if (logger.isFinestEnabled()) {
                        logger.finest("PartitionId=" + partition.getPartitionId() + " " + replica
                                + " is removed from replica index: " + i + ", partition: " + partition);
                    }
                }
            }
        }
    }

    boolean isAbsentInPartitionTable(Member member) {
        PartitionReplica replica = PartitionReplica.from(member);
        for (InternalPartitionImpl partition : partitions) {
            if (partition.isOwnerOrBackup(replica)) {
                return false;
            }
        }
        return true;
    }

    InternalPartition[] getPartitions() {
        return partitions;
    }

    /**
     * Returns a copy of the current partition table.
     */
    public InternalPartition[] getPartitionsCopy(boolean readonly) {
        NopPartitionReplicaInterceptor interceptor = new NopPartitionReplicaInterceptor();
        InternalPartition[] result = new InternalPartition[partitions.length];
        for (int i = 0; i < partitionCount; i++) {
            if (readonly) {
                result[i] = new ReadonlyInternalPartition(partitions[i]);
            } else {
                result[i] = partitions[i].copy(interceptor);
            }
        }
        return result;
    }

    public InternalPartitionImpl getPartitionImpl(int partitionId) {
        return partitions[partitionId];
    }

    PartitionReplica[][] repartition(Set<Member> excludedMembers, Collection<Integer> partitionInclusionSet) {
        if (!initialized) {
            return null;
        }
        Collection<MemberGroup> memberGroups = createMemberGroups(excludedMembers);
        PartitionReplica[][] newState = partitionStateGenerator.arrange(memberGroups, partitions, partitionInclusionSet);

        if (newState == null) {
            if (logger.isFinestEnabled()) {
                logger.finest("Partition rearrangement failed. Number of member groups: " + memberGroups.size());
            }
        }

        return newState;
    }

    public boolean trySetMigratingFlag(int partitionId) {
        if (logger.isFinestEnabled()) {
            logger.finest("Setting partition-migrating flag. partitionId=" + partitionId);
        }
        return partitions[partitionId].setMigrating();
    }

    public void clearMigratingFlag(int partitionId) {
        if (logger.isFinestEnabled()) {
            logger.finest("Clearing partition-migrating flag. partitionId=" + partitionId);
        }
        partitions[partitionId].resetMigrating();
    }

    public boolean isMigrating(int partitionId) {
        return partitions[partitionId].isMigrating();
    }

    public void updateStamp() {
        stateStamp = calculateStamp(partitions, () -> stampCalculationBuffer);
        if (logger.isFinestEnabled()) {
            logger.finest("New calculated partition state stamp is: " + stateStamp);
        }
    }

    public long getStamp() {
        return stateStamp;
    }

    public int getPartitionVersion(int partitionId) {
        return partitions[partitionId].version();
    }

    /**
     * Increments partition version by delta and updates partition state stamp.
     */
    void incrementPartitionVersion(int partitionId, int delta) {
        InternalPartitionImpl partition = partitions[partitionId];
        partition.setVersion(partition.version() + delta);
        updateStamp();
    }

    boolean setInitialized() {
        if (!initialized) {
            updateStamp();
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
        stateStamp = INITIAL_STAMP;
        // local member uuid changes during ClusterService reset
        PartitionReplica localReplica = PartitionReplica.from(node.getLocalMember());
        for (InternalPartitionImpl partition : partitions) {
            partition.reset(localReplica);
        }
    }

    int replaceMember(Member oldMember, Member newMember) {
        if (!initialized) {
            return 0;
        }
        PartitionReplica oldReplica = PartitionReplica.from(oldMember);
        PartitionReplica newReplica = PartitionReplica.from(newMember);

        int count = 0;
        for (InternalPartitionImpl partition : partitions) {
            if (partition.replaceReplica(oldReplica, newReplica) > -1) {
                count++;
            }
        }
        if (count > 0) {
            node.getNodeExtension().onPartitionStateChange();
            logger.info("Replaced " + oldMember + " with " + newMember + " in partition table in "
                    + count + " partitions.");
        }
        return count;
    }

    PartitionTableView getPartitionTable() {
        return new PartitionTableView(getPartitionsCopy(true));
    }

    void storeSnapshot(UUID crashedMemberUuid) {
        logger.info("Storing snapshot of partition assignments while removing UUID " + crashedMemberUuid);
        snapshotOnRemove.put(crashedMemberUuid, getPartitionTable());
    }

    Collection<PartitionTableView> snapshots() {
        return Collections.unmodifiableCollection(snapshotOnRemove.values());
    }

    PartitionTableView getSnapshot(UUID crashedMemberUuid) {
        return snapshotOnRemove.get(crashedMemberUuid);
    }
}
