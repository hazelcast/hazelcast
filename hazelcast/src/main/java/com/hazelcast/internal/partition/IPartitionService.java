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

import com.hazelcast.partition.MigrationListener;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.internal.services.CoreService;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * An SPI service for accessing partition related information.
 */
public interface IPartitionService extends CoreService {

    /**
     * The name of the service.
     */
    String SERVICE_NAME = "hz:core:partitionService";

    /**
     * Gets the owner of the partition if it's set.
     * <p>
     * If the owner of the partition is not set yet, it will trigger partition assignment.
     *
     * @param partitionId the partitionId
     * @return owner of partition or {@code null} if it's not set yet
     */
    Address getPartitionOwner(int partitionId);

    /**
     * Gets the owner of the partition.
     * <p>
     * If none is set, it will wait till the owner is set.
     *
     * @param partitionId the partitionId
     * @return owner of partition
     * @throws InterruptedException if interrupted while waiting
     * @throws NoDataMemberInClusterException if all nodes are lite members and partitions can't be assigned
     */
    Address getPartitionOwnerOrWait(int partitionId);

    /**
     * Returns the {@link IPartition} for a given partition ID.
     * <p>
     * If the owner of the partition is not set yet, it will trigger partition assignment.
     * <p>
     * The {@link IPartition} for a given partition ID will never change, so the result can be cached safely.
     *
     * @param partitionId the partition ID
     * @return the IPartition
     */
    IPartition getPartition(int partitionId);

    /**
     * Returns the {@link IPartition} for a given partition ID.
     * <p>
     * If the owner of the partition is not set yet and {@code triggerOwnerAssignment} is {@code true},
     * it will trigger partition assignment.
     * <p>
     * The {@link IPartition} for a given partition ID will never change, so the result can be cached safely.
     *
     * @param partitionId            the partition ID
     * @param triggerOwnerAssignment flag to trigger the partition assignment
     * @return the IPartition
     */
    IPartition getPartition(int partitionId, boolean triggerOwnerAssignment);

    /**
     * Returns the partition ID for a {@link Data} key.
     *
     * @param key the {@link Data} key
     * @return the partition ID
     * @throws NullPointerException if key is {@code null}
     */
    int getPartitionId(@Nonnull Data key);

    /**
     * Returns the partition ID for a given object.
     *
     * @param key the object key
     * @return the partition ID
     */
    int getPartitionId(@Nonnull Object key);

    /**
     * Returns the number of partitions.
     *
     * @return the number of partitions
     */
    int getPartitionCount();

    /**
     * Returns partition ID list assigned to given target.
     * <p>
     * If the owner of the partition is not set yet, it will trigger partition assignment.
     *
     * @return partition ID list assigned to given target
     */
    List<Integer> getMemberPartitions(Address target);

    /**
     * Gets member partition IDs.
     * <p>
     * Blocks until partitions are assigned.
     *
     * @return map of member address to partition IDs
     */
    Map<Address, List<Integer>> getMemberPartitionsMap();

    UUID addMigrationListener(MigrationListener migrationListener);

    CompletableFuture<UUID> addMigrationListenerAsync(MigrationListener migrationListener);

    UUID addLocalMigrationListener(MigrationListener migrationListener);

    boolean removeMigrationListener(UUID registrationId);

    CompletableFuture<Boolean> removeMigrationListenerAsync(UUID registrationId);

    UUID addPartitionLostListener(PartitionLostListener partitionLostListener);

    CompletableFuture<UUID> addPartitionLostListenerAsync(PartitionLostListener partitionLostListener);

    UUID addLocalPartitionLostListener(PartitionLostListener partitionLostListener);

    boolean removePartitionLostListener(UUID registrationId);

    CompletableFuture<Boolean> removePartitionLostListenerAsync(UUID registrationId);

    long getMigrationQueueSize();

    /**
     * Queries and returns if this member in a safe state or not.
     * <p>
     * This method just checks for a safe state, it doesn't force this member to be in a safe state.
     *
     * @return {@code true} if this member in a safe state, otherwise {@code false}
     */
    boolean isMemberStateSafe();

    /**
     * Returns maximum allowed backup count according to current
     * cluster formation and partition group configuration.
     * <p>
     * The returned number will be in the range of [0, {@link IPartition#MAX_BACKUP_COUNT}].
     *
     * @return max allowed backup count
     */
    int getMaxAllowedBackupCount();

    /**
     * Returns stamp of the current partition state.
     */
    long getPartitionStateStamp();

    /**
     * Checks if there are any cluster-wide migrations.
     *
     * @return {@code true} if there are migrations, {@code false} otherwise.
     */
    boolean hasOnGoingMigration();

    /**
     * Checks if there are any local migrations.
     *
     * @return {@code true} if there are migrations, {@code false} otherwise.
     */
    boolean hasOnGoingMigrationLocal();

    /**
     * Check if this node is the owner of a partition.
     *
     * @param partitionId the partition ID
     * @return {@code true} if the node owns the partition,
     * {@code false} if the node doesn't own the partition or if the partition hasn't been assigned yet
     */
    boolean isPartitionOwner(int partitionId);

    /**
     * Returns an array of {@link IPartition} instances.
     * <p>
     * <b>Note</b>: Creates a new array on each invocation, not recommended to use in high-loaded parts of code.
     *
     * @return array of {@link IPartition} instances
     */
    IPartition[] getPartitions();
}
