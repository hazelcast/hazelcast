/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.Node;
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
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.partitiongroup.MemberGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.IntConsumer;

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
public class PartitionStateManagerImpl implements PartitionStateManager {

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

    /**
     * For test usage only
     */
    private ReplicaUpdateInterceptor replicaUpdateInterceptor;

    public PartitionStateManagerImpl(Node node, InternalPartitionServiceImpl partitionService) {
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
        this.replicaUpdateInterceptor = NoOpBatchReplicaUpdateInterceptor.INSTANCE;
    }

    @Override
    public boolean hasMigratingPartitions() {
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
        Collection<Member> members = node.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        return memberGroupFactory.createMemberGroups(members);
    }

    public Collection<MemberGroup> createMemberGroups(Collection<Member> members) {
        List<Member> dataMembers = new ArrayList<>();
        for (Member member : members) {
            if (DATA_MEMBER_SELECTOR.select(member)) {
                dataMembers.add(member);
            }
        }
        return memberGroupFactory.createMemberGroups(dataMembers);
    }

    @Override
    public boolean initializePartitionAssignments(Set<Member> excludedMembers) {
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

        batchUpdateReplicas(newState);

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

    void batchUpdateReplicas(PartitionReplica[][] newState) {
        PartitionIdSet changedOwnersSet = new PartitionIdSet(partitionCount);
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartitionImpl partition = partitions[partitionId];
            PartitionReplica[] replicas = newState[partitionId];
            if (partition.setReplicas(replicas, false)) {
                changedOwnersSet.add(partitionId);
            }
        }
        partitionOwnersChanged(changedOwnersSet);
    }

    @Override
    public void partitionOwnersChanged(PartitionIdSet partitionIdSet) {
        partitionIdSet.intIterator().forEachRemaining(
                (IntConsumer) partitionId -> partitionService.getReplicaManager().cancelReplicaSync(partitionId));
        updateStamp();
        replicaUpdateInterceptor.onPartitionOwnersChanged();
    }

    /**
     * Returns {@code true} if the node has started and
     * the cluster state allows migrations (see {@link ClusterState#isMigrationAllowed()}).
     */
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

    @Override
    public void setInitialState(PartitionTableView partitionTable) {
        if (initialized) {
            throw new IllegalStateException("Partition table is already initialized!");
        }
        logger.info("Setting cluster partition table...");
        boolean foundReplica = false;
        PartitionReplica localReplica = PartitionReplica.from(node.getLocalMember());
        PartitionIdSet changedOwnerPartitions = new PartitionIdSet(partitionCount);
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
                if (partition.setReplicasAndVersion(newPartition)) {
                    changedOwnerPartitions.add(partitionId);
                }
            }
        }
        if (foundReplica) {
            partitionOwnersChanged(changedOwnerPartitions);
            setInitialized();
        }
    }

    @Override
    public void updateMemberGroupsSize() {
        final Collection<MemberGroup> groups = createMemberGroups();
        int size = 0;
        for (MemberGroup group : groups) {
            if (group.size() > 0) {
                size++;
            }
        }
        memberGroupsSize = size;
    }

    @Override
    public int getMemberGroupsSize() {
        int size = memberGroupsSize;
        if (size > 0) {
            return size;
        }

        // size = 0 means service is not initialized yet.
        // return 1 if current node is a data member since there should be at least one member group
        return node.isLiteMember() ? 0 : 1;
    }

    @Override
    public void removeUnknownAndLiteMembers() {
        ClusterServiceImpl clusterService = node.getClusterService();

        for (InternalPartitionImpl partition : partitions) {
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                PartitionReplica replica = partition.getReplica(i);
                if (replica == null) {
                    continue;
                }

                Member member = clusterService.getMember(replica.address(), replica.uuid());
                if (member == null || member.isLiteMember()) {
                    partition.setReplica(i, null);
                    if (logger.isFinestEnabled()) {
                        logger.finest("PartitionId=" + partition.getPartitionId() + " " + replica
                                + " is removed from replica index: " + i + ", partition: " + partition);
                    }
                }
            }
        }
    }

    @Override
    public boolean isAbsentInPartitionTable(Member member) {
        PartitionReplica replica = PartitionReplica.from(member);
        for (InternalPartitionImpl partition : partitions) {
            if (partition.isOwnerOrBackup(replica)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public InternalPartition[] getPartitions() {
        return partitions;
    }

    @Override
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

    @Override
    public InternalPartitionImpl getPartitionImpl(int partitionId) {
        return partitions[partitionId];
    }

    @Override
    public PartitionReplica[][] repartition(Set<Member> excludedMembers, Collection<Integer> partitionInclusionSet) {
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

    @Override
    public boolean trySetMigratingFlag(int partitionId) {
        if (logger.isFinestEnabled()) {
            logger.finest("Setting partition-migrating flag. partitionId=" + partitionId);
        }
        return partitions[partitionId].setMigrating();
    }

    @Override
    public void clearMigratingFlag(int partitionId) {
        if (logger.isFinestEnabled()) {
            logger.finest("Clearing partition-migrating flag. partitionId=" + partitionId);
        }
        if (!isMigrating(partitionId)) {
            // If this warning is generated it means that trySetMigratingFlag and clearMigratingFlag calls
            // were mismatched. It may indicate that there was a period of time when migration or replica
            // sync was running and mutating operations were allowed. This means that there is a risk of
            // data loss or inconsistency.
            logger.warning("Partition " + partitionId + " is not migrating");
        }
        partitions[partitionId].resetMigrating();
    }

    @Override
    public boolean isMigrating(int partitionId) {
        return partitions[partitionId].isMigrating();
    }

    @Override
    public void updateStamp() {
        stateStamp = calculateStamp(partitions, () -> stampCalculationBuffer);
        if (logger.isFinestEnabled()) {
            logger.finest("New calculated partition state stamp is: " + stateStamp);
        }
        replicaUpdateInterceptor.onPartitionStampUpdate();
    }

    @Override
    public long getStamp() {
        return stateStamp;
    }

    @Override
    public int getPartitionVersion(int partitionId) {
        return partitions[partitionId].version();
    }

    @Override
    public void incrementPartitionVersion(int partitionId, int delta) {
        InternalPartitionImpl partition = partitions[partitionId];
        partition.setVersion(partition.version() + delta);
        updateStamp();
    }

    @Override
    public boolean setInitialized() {
        if (!initialized) {
            // partition state stamp is already calculated
            assert stateStamp != 0 : "Partition state stamp should already have been calculated";
            initialized = true;
            node.getNodeExtension().onPartitionStateChange();
            return true;
        }
        return false;
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public void reset() {
        initialized = false;
        stateStamp = INITIAL_STAMP;
        // local member uuid changes during ClusterService reset
        PartitionReplica localReplica = PartitionReplica.from(node.getLocalMember());
        for (InternalPartitionImpl partition : partitions) {
            partition.reset(localReplica);
        }
    }

    @Override
    public int replaceMember(Member oldMember, Member newMember) {
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

    @Override
    public PartitionTableView getPartitionTable() {
        return new PartitionTableView(getPartitionsCopy(true));
    }

    @Override
    public void storeSnapshot(UUID crashedMemberUuid) {
        logger.info("Storing snapshot of partition assignments while removing UUID " + crashedMemberUuid);
        snapshotOnRemove.put(crashedMemberUuid, getPartitionTable());
    }

    @Override
    public Collection<PartitionTableView> snapshots() {
        return Collections.unmodifiableCollection(snapshotOnRemove.values());
    }

    @Override
    public PartitionTableView getSnapshot(UUID crashedMemberUuid) {
        return snapshotOnRemove.get(crashedMemberUuid);
    }

    @Override
    public void removeSnapshot(UUID memberUuid) {
        snapshotOnRemove.remove(memberUuid);
    }

    @Override
    public void setReplicaUpdateInterceptor(ReplicaUpdateInterceptor interceptor) {
        this.replicaUpdateInterceptor = interceptor;
    }

    static final class NoOpBatchReplicaUpdateInterceptor implements ReplicaUpdateInterceptor {

        static final NoOpBatchReplicaUpdateInterceptor INSTANCE = new NoOpBatchReplicaUpdateInterceptor();

        @Override
        public void onPartitionOwnersChanged() {
        }

        @Override
        public void onPartitionStampUpdate() {
        }
    }
}
