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

package com.hazelcast.partition;

import com.hazelcast.cluster.Member;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * PartitionService allows you to query {@link Partition}s and attach/detach {@link MigrationListener}s to listen to partition
 * migration events.
 * <p>
 * The methods on the PartitionService are thread-safe.
 *
 * @see Partition
 * @see MigrationListener
 * @see PartitionLostListener
 */
public interface PartitionService {

    /**
     * Returns a set containing all the {@link Partition}s in the cluster.
     *
     * @return all partitions in the cluster
     */
    Set<Partition> getPartitions();

    /**
     * Returns the partition that the given key belongs to.
     *
     * @param key the given key
     * @return the partition that the given key belongs to
     */
    Partition getPartition(@Nonnull Object key);

    /**
     * Adds a MigrationListener.
     * <p>
     * The addMigrationListener returns a register ID. This ID is needed to remove the MigrationListener using the
     * {@link #removeMigrationListener(UUID)} method.
     * <p>
     * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
     *
     * @param migrationListener the added MigrationListener
     * @return returns the registration ID for the MigrationListener
     * @throws java.lang.NullPointerException if migrationListener is {@code null}
     * @throws UnsupportedOperationException  if this operation isn't supported. For example on the client side it isn't possible
     *                                        to add a MigrationListener
     * @see #removeMigrationListener(UUID)
     */
    UUID addMigrationListener(MigrationListener migrationListener);

    /**
     * Removes a MigrationListener.
     * <p>
     * If the same MigrationListener is registered multiple times, it needs to be removed multiple times.
     * <p>
     * This method can safely be called multiple times for the same registration ID; every subsequent call is just ignored.
     *
     * @param registrationId the registration ID of the listener to remove
     * @return {@code true} if the listener is removed, {@code false} otherwise
     * @throws java.lang.NullPointerException if registration ID is {@code null}
     * @throws UnsupportedOperationException  if this operation isn't supported, e.g. on the client side it isn't possible
     *                                        to add/remove a MigrationListener
     * @see #addMigrationListener(MigrationListener)
     */
    boolean removeMigrationListener(UUID registrationId);


    /**
     * Adds a PartitionLostListener.
     * <p>
     * The addPartitionLostListener returns a registration ID. This ID is needed to remove the PartitionLostListener using the
     * {@link #removePartitionLostListener(UUID)} method.
     * <p>
     * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
     * IMPORTANT: Please @see com.hazelcast.partition.PartitionLostListener for weaknesses
     *
     * @param partitionLostListener the added PartitionLostListener
     * @return returns the registration ID for the PartitionLostListener
     * @throws java.lang.NullPointerException if partitionLostListener is {@code null}
     * @see #removePartitionLostListener(UUID)
     */
    UUID addPartitionLostListener(PartitionLostListener partitionLostListener);

    /**
     * Removes a PartitionLostListener.
     * <p>
     * If the same PartitionLostListener is registered multiple times, it needs to be removed multiple times.
     * <p>
     * This method can safely be called multiple times for the same registration ID; every subsequent call is just ignored.
     *
     * @param registrationId the registration ID of the listener to remove
     * @return {@code true} if the listener is removed, {@code false} otherwise
     * @throws java.lang.NullPointerException if registration ID is {@code null}
     * @see #addPartitionLostListener(PartitionLostListener)
     */
    boolean removePartitionLostListener(UUID registrationId);

    /**
     * Checks whether the cluster is in a safe state.
     * <p>
     * Safe state means; there are no partitions being migrated and all backups are in sync
     * when this method is called.
     *
     * @return {@code true} if there are no partitions being migrated and all backups are in sync, {@code false} otherwise
     * @since 3.3
     */
    boolean isClusterSafe();

    /**
     * Checks whether the given member is in safe state.
     * <p>
     * Safe state means; all backups of partitions currently owned by the member are in sync when this method is called.
     *
     * @param member the cluster member to query
     * @return {@code true} if the member is in a safe state, {@code false} otherwise
     * @since 3.3
     */
    boolean isMemberSafe(Member member);

    /**
     * Checks whether local member is in safe state.
     * <p>
     * Safe state means; all backups of partitions currently owned by local member are in sync when this method is called.
     *
     * @since 3.3
     */
    boolean isLocalMemberSafe();

    /**
     * Force the local member to be safe by checking and syncing partitions owned by the local member
     * with at least one of the backups.
     *
     * @param timeout the time limit for checking/syncing with the backup
     * @param unit    the unit of time for timeout
     * @since 3.3
     */
    boolean forceLocalMemberToBeSafe(long timeout, TimeUnit unit);
}
