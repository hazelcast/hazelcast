/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.partition.operation.FinalizeMigrationOperation;
import com.hazelcast.internal.partition.operation.PartitionReplicaSyncRequest;
import com.hazelcast.internal.partition.operation.PartitionStateOperation;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface MigrationManager {
    long getPartitionMigrationInterval();

    void pauseMigration();

    void resumeMigration();

    /**
     * Checks if migration tasks are allowed. This can include partition state and partition data sync tasks.
     * The migration is not allowed during membership changes (member removed or joining) or for a shorter period when
     * a migration fails before restarting the migration process.
     *
     * @see MigrationRunnable
     * @see PublishPartitionRuntimeStateTask
     * @see PartitionStateOperation
     * @see PartitionReplicaSyncRequest
     */
    boolean areMigrationTasksAllowed();

    /**
     * Finalizes a migration that has finished with {@link MigrationInfo.MigrationStatus#SUCCESS}
     * or {@link MigrationInfo.MigrationStatus#FAILED} by invoking {@link FinalizeMigrationOperation}
     * locally if this is the source or destination. The finalization is asynchronous
     * and there might be other ongoing migration finalizations.
     * <p>
     * It will also cleanup the migration state by removing the active migration and
     * clearing the migration flag on the partition owner.
     * <p>
     * This method should not be called on a node which is not the source, destination
     * or partition owner for this migration.
     *
     * @param migrationInfo the migration to be finalized
     */
    void finalizeMigration(MigrationInfo migrationInfo);

    boolean isChunkedMigrationEnabled();

    int getMaxTotalChunkedDataInBytes();

    boolean removeFinalizingMigration(MigrationInfo migration);

    boolean isFinalizingMigrationRegistered(int partitionId);

    /**
     * Adds the active migration if none is set for the partition and returns {@code null},
     * otherwise returns the currently set active migration.
     */
    MigrationInfo addActiveMigration(MigrationInfo migrationInfo);

    MigrationInfo getActiveMigration(int partitionId);

    Collection<MigrationInfo> getActiveMigrations();

    /**
     * Acquires promotion commit permit which is needed while running promotion commit
     * to prevent concurrent commits.
     * <p>
     * Normally, promotions are submitted &amp; executed serially
     * but when the commit operation timeouts, it's retried which can cause concurrent execution
     * (promotion commit operation runs on generic operation threads).
     * <p>
     * Promotion commit operation is idempotent when executed serially.
     *
     * @return true if promotion commit is allowed to run, false otherwise
     */
    boolean acquirePromotionPermit();

    /**
     * Releases promotion commit permit.
     *
     * @see #acquirePromotionPermit()
     */
    void releasePromotionPermit();

    /**
     * Finalizes the active migration if it is equal to the {@code migrationInfo} or if this node was a backup replica before
     * the migration (see {@link FinalizeMigrationOperation}).
     * Acquires the partition service lock.
     */
    void scheduleActiveMigrationFinalization(MigrationInfo migrationInfo);

    /**
     * Adds the migration to the set of completed migrations and increases the completed migration counter.
     * Acquires the partition service lock to update the migrations.
     *
     * @param migrationInfo the completed migration
     * @return {@code true} if the migration has been added or {@code false} if this migration is already in the completed set
     * @throws IllegalArgumentException if the migration is not completed
     */
    boolean addCompletedMigration(MigrationInfo migrationInfo);

    /**
     * Retains only the {@code migrations} in the completed migration list. Acquires the partition service lock.
     */
    void retainCompletedMigrations(Collection<MigrationInfo> migrations);

    /**
     * Clears the migration queue and triggers the control task. Called on the master node.
     */
    void triggerControlTask();

    void triggerControlTaskWithDelay();

    MigrationInterceptor getMigrationInterceptor();

    void setMigrationInterceptor(MigrationInterceptor interceptor);

    void resetMigrationInterceptor();

    boolean onDemoteRequest(Member member);

    void onShutdownRequest(Member member);

    void onMemberRemove(Member member);

    void schedule(MigrationRunnable runnable);

    /**
     * Returns a copy of the list of completed migrations. Runs under the partition service lock.
     */
    List<MigrationInfo> getCompletedMigrationsCopy();

    boolean hasOnGoingMigration();

    int getMigrationQueueSize();

    void reset();

    void start();

    void stop();

    /**
     * Schedules a migration by adding it to the migration queue.
     */
    void scheduleMigration(MigrationInfo migrationInfo);

    Set<Member> getDataDisownRequestedMembers();

    /**
     * Returns {@code true} if a repartitioning action occurred (member removal or addition)
     * while migrations are not allowed by current cluster state
     * (such as {@link ClusterState#NO_MIGRATION}, {@link ClusterState#PASSIVE}),
     * {@code false} otherwise.
     */
    boolean shouldTriggerRepartitioningWhenClusterStateAllowsMigration();

    MigrationStats getStats();
}
