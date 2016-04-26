/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.partition;

import com.hazelcast.core.MigrationListener;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.spi.CoreService;

import java.util.List;
import java.util.Map;

/**
 * A SPI service for accessing partition related information.
 */
public interface IPartitionService extends CoreService {

    /**
     * The name of the service.
     */
    String SERVICE_NAME = "hz:core:partitionService";

    /**
     * Gets the owner of the partition if it's set.
     * Otherwise it will trigger partition assignment.
     *
     * @param partitionId the partitionId
     * @return owner of partition or null if it's not set yet.
     */
    Address getPartitionOwner(int partitionId);

    /**
     * Gets the owner of the partition. If none is set, it will wait till the owner is set.
     *
     * @param partitionId  the partitionId
     * @return owner of partition
     * @throws InterruptedException
     * @throws NoDataMemberInClusterException if all nodes are lite members and partitions can't be assigned
     */
    Address getPartitionOwnerOrWait(int partitionId);

    /**
     * Returns the IPartition for a given partitionId.
     * If owner of the partition is not set yet, it will trigger partition assignment.
     * <p/>
     * The IPartition for a given partitionId will never change, so it can be cached safely.
     *
     * @param partitionId the partitionId
     * @return the IPartition.
     */
    IPartition getPartition(int partitionId);

    /**
     * Returns the IPartition for a given partitionId.
     * If owner of the partition is not set yet and {@code triggerOwnerAssignment} is true,
     * it will trigger partition assignment.
     * <p/>
     * The IPartition for a given partitionId will never change, so it can be cached safely.
     *
     * @param partitionId the partitionId
     * @param triggerOwnerAssignment flag to trigger partition assignment
     * @return the IPartition.
     */
    IPartition getPartition(int partitionId, boolean triggerOwnerAssignment);

    /**
     * Returns the partition id for a Data key.
     *
     * @param key the Data key.
     * @return the partition id.
     * @throws NullPointerException if key is null.
     */
    int getPartitionId(Data key);

    /**
     * Returns the partition id for a given object.
     *
     * @param key the object key.
     * @return the partition id.
     */
    int getPartitionId(Object key);

    /**
     * Returns the number of partitions.
     *
     * @return the number of partitions.
     */
    int getPartitionCount();

    List<Integer> getMemberPartitions(Address target);

    /**
     * Gets member partition IDs. Blocks until partitions are assigned.
     * @return map of member address to partition Ids
     **/
    Map<Address, List<Integer>> getMemberPartitionsMap();

    String addMigrationListener(MigrationListener migrationListener);

    boolean removeMigrationListener(String registrationId);

    String addPartitionLostListener(PartitionLostListener partitionLostListener);

    String addLocalPartitionLostListener(PartitionLostListener partitionLostListener);

    boolean removePartitionLostListener(String registrationId);

    long getMigrationQueueSize();

    /**
     * Query and return if this member in a safe state or not.
     * This method just checks for a safe state, it doesn't force this member to be in a safe state.
     *
     * @return <code>true</code> if this member in a safe state, otherwise <code>false</code>
     */
    boolean isMemberStateSafe();

    /**
     * Returns maximum allowed backup count according to current
     * cluster formation and partition group configuration.
     * <p/>
     * Returned number will be in range of [0, {@link IPartition#MAX_BACKUP_COUNT}].
     *
     * @return max allowed backup count
     */
    int getMaxAllowedBackupCount();

    int getPartitionStateVersion();

    /**
     * Checks if there are any cluster-wide migrations.
     *
     * @return true if there are migrations, false otherwise.
     */
    boolean hasOnGoingMigration();

    /**
     * Checks if there are any local migrations.
     *
     * @return true if there are migrations, false otherwise.
     */
    boolean hasOnGoingMigrationLocal();

    /**
     * Check if this node is the owner of a partition
     * @param partitionId
     * @return true if it owns the partition. false if it doesn't or the partition hasn't been assigned yet.
     */
    boolean isPartitionOwner(int partitionId);

    /**
     * @return copy of array with IPartition objects
     * create new array on each invocation, not recommended to use in high-loaded parts of code
     */
    IPartition[] getPartitions();
}
