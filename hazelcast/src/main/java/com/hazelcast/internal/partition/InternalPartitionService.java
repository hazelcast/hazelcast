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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionReplicaStateChecker;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.partition.operation.FetchPartitionStateOperation;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.services.GracefulShutdownAwareService;
import com.hazelcast.internal.services.ManagedService;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

public interface InternalPartitionService extends IPartitionService, ManagedService, GracefulShutdownAwareService {

    /**
     * Static constant for dispatching and listening migration events
     */
    String MIGRATION_EVENT_TOPIC = ".migration";

    /**
     * Static constant for dispatching and listening internal partition lost events
     */
    String PARTITION_LOST_EVENT_TOPIC = ".partitionLost";

    @Override
    InternalPartition getPartition(int partitionId);

    @Override
    InternalPartition getPartition(int partitionId, boolean triggerOwnerAssignment);

    /**
     * Number of the member groups to be used in partition assignments.
     *
     * @see com.hazelcast.internal.partition.membergroup.MemberGroupFactory
     * @see com.hazelcast.config.PartitionGroupConfig
     * @return number of member groups
     */
    int getMemberGroupsSize();

    /**
     * Pause all migrations
     */
    void pauseMigration();

    /**
     * Resume all migrations
     */
    void resumeMigration();

    /**
     * Called when a member is added to the cluster. Triggers partition rebalancing.
     * @param member new member
     */
    void memberAdded(Member member);

    /**
     * Called when some members are removed from the cluster.
     * Executes maintenance tasks, removes the members from the partition table and triggers promotions.
     * @param members removed members
     */
    void memberRemoved(Member... members);

    InternalPartition[] getInternalPartitions();

    /**
     * Causes the partition table to be arranged and published to members if :
     * <ul>
     * <li>this instance has started</li>
     * <li>this instance is the master</li>
     * <li>the cluster is {@link ClusterState#ACTIVE}</li>
     * <li>if the partition table has not already been arranged</li>
     * <li>if there is no cluster membership change</li>
     * </ul>
     * If this instance is not the master, it will trigger the master to assign the partitions.
     *
     * @return {@link PartitionRuntimeState} if this node is the master and the partition table is initialized
     * @throws HazelcastException if the partition state generator failed to arrange the partitions
     * @see PartitionStateManager#initializePartitionAssignments(java.util.Set)
     */
    PartitionRuntimeState firstArrangement();

    /**
     * Creates the current partition runtime state. May return {@code null} if the node should fetch the most recent partition
     * table (e.g. this node is a newly appointed master) or if the partition state manager is not initialized.
     *
     * @return the current partition state
     * @see InternalPartitionServiceImpl#isFetchMostRecentPartitionTableTaskRequired()
     * @see FetchPartitionStateOperation
     * @see PartitionStateManager#isInitialized()
     */
    PartitionRuntimeState createPartitionState();

    PartitionReplicaVersionManager getPartitionReplicaVersionManager();

    /**
     * Creates an immutable/readonly view of partition table.
     * @return immutable view of partition table
     */
    PartitionTableView createPartitionTableView();

    /**
     * Returns partition ID list assigned to given target if partitions are assigned when method is called.
     * Does not trigger partition assignment otherwise.
     *
     * @return partition ID list assigned to given target if partitions are assigned already
     */
    List<Integer> getMemberPartitionsIfAssigned(Address target);

    /**
     * Returns the {@link PartitionServiceProxy} of the partition service..
     *
     * @return the {@link PartitionServiceProxy}
     */
    PartitionServiceProxy getPartitionServiceProxy();

    PartitionReplicaStateChecker getPartitionReplicaStateChecker();

    @Nullable
    PartitionTableView getLeftMemberSnapshot(UUID uuid);
}
