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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.partition.operation.FetchPartitionStateOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.partition.IPartitionService;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface InternalPartitionService extends IPartitionService {

    /**
     * Retry count for migration operations.
     * <p>
     * Current Invocation mechanism retries first 5 invocations without pausing.
     */
    int MIGRATION_RETRY_COUNT = 12;

    /**
     * Retry pause for migration operations in milliseconds.
     */
    long MIGRATION_RETRY_PAUSE = 10000;

    /**
     * Static constant for dispatching and listening migration events
     */
    String MIGRATION_EVENT_TOPIC = ".migration";

    /**
     * Static constant for dispatching and listening internal partition lost events
     */
    String PARTITION_LOST_EVENT_TOPIC = ".partitionLost";

    InternalPartition getPartition(int partitionId);

    InternalPartition getPartition(int partitionId, boolean triggerOwnerAssignment);

    int getMemberGroupsSize();

    /**
     * Pause all migrations
     */
    void pauseMigration();

    /**
     * Resume all migrations
     */
    void resumeMigration();

    boolean isMemberAllowedToJoin(Address address);

    void memberAdded(MemberImpl newMember);

    void memberRemoved(MemberImpl deadMember);

    boolean prepareToSafeShutdown(long timeout, TimeUnit seconds);

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
     * @throws HazelcastException if the partition state generator failed to arrange the partitions
     * @return {@link PartitionRuntimeState} if this node is the master and the partition table is initialized
     *
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

    PartitionTableView createPartitionTableView();

    /**
     * Returns partition ID list assigned to given target if partitions are assigned when method is called.
     * Does not trigger partition assignment otherwise.
     *
     * @return partition ID list assigned to given target if partitions are assigned already
     */
    List<Integer> getMemberPartitionsIfAssigned(Address target);
}
