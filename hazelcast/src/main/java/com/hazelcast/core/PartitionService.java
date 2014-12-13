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

package com.hazelcast.core;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * PartitionService allows you to query {@link Partition}s and attach/detach {@link MigrationListener}s to listen to partition
 * migration events.
 *
 * The methods on the PartitionService are thread-safe.
 *
 * @see Partition
 * @see MigrationListener
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
     * todo: what about null.
     */
    Partition getPartition(Object key);

    /**
     * Generates a random partition key. This is useful if you want to partition data in the same partition,
     * but you do not care which partition it is going to be.
     * <p/>
     * The returned value will never be null.
     *
     * @return the random partition key.
     */
    String randomPartitionKey();

    /**
     * Adds a MigrationListener.
     * <p/>
     * The addMigrationListener returns a register-id. This id is needed to remove the MigrationListener using the
     * {@link #removeMigrationListener(String)} method.
     * <p/>
     * There is no check for duplicate registrations, so if you register the listener twice, it will get events twice.
     *
     * @param migrationListener the added MigrationListener
     * @return returns the registration id for the MigrationListener.
     * @throws java.lang.NullPointerException if migrationListener is null.
     * @throws UnsupportedOperationException  if this operation isn't supported. For example on the client side it isn't possible
     *                                        to add a MigrationListener.
     * @see #removeMigrationListener(String)
     */
    String addMigrationListener(MigrationListener migrationListener);

    /**
     * Removes a MigrationListener.
     * <p/>
     * If the same MigrationListener is registered multiple times, it needs to be removed multiple times.
     * <p/>
     * This method can safely be called multiple times for the same registration-id; every subsequent call is just ignored.
     *
     * @param registrationId the registration id of the listener to remove.
     * @return true if the listener is removed, false otherwise.
     * @throws java.lang.NullPointerException if registrationId is null.
     * @throws UnsupportedOperationException  if this operation isn't supported. For example, on the client side it isn't possible
     *                                        to add/remove a MigrationListener.
     * @see #addMigrationListener(MigrationListener)
     */
    boolean removeMigrationListener(String registrationId);

    /**
     * Checks whether the cluster is in a safe state. When in a safe state,
     * it is permissible to shut down a server instance.
     *
     * @return true if there are no partitions being migrated, and there are sufficient backups
     * for each partition per the configuration; false otherwise.
     * @since 3.3
     */
    boolean isClusterSafe();

    /**
     * Check if the given member is safe to shutdown, meaning check if at least one backup of the partitions
     * owned by the given member are in sync with primary.
     *
     * @param member the cluster member to query.
     * @return true if the member is in a safe state, false otherwise.
     * @since 3.3
     */
    boolean isMemberSafe(Member member);

    /**
     * Check if the local member is safe to shutdown, meaning check if at least one backup of the partitions
     * owned by the local member are in sync with primary.
     *
     * @since 3.3
     */
    boolean isLocalMemberSafe();

    /**
     * Force the local member to be safe by checking and syncing partitions owned by the local member
     * with at least one of the backups.
     * @param timeout the time limit for checking/syncing with the backup
     * @param unit the unit of time for timeout
     *
     * @since 3.3
     */
    boolean forceLocalMemberToBeSafe(long timeout, TimeUnit unit);
}
