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

import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * An interface that can be implemented by SPI services to get notified of partition changes; for example, if a
 * {@link com.hazelcast.map.impl.MapService} starts moving its data around because partitions are moving
 * to a different machine.
 * <p>
 * Service itself must decide to keep or remove its data for specific partition according to number of backups
 * it's willing to keep and {@code PartitionMigrationEvent} parameters; partitionId, migration-endpoint,
 * current replica index and new replica index.
 * <ul>
 * <li>Migration endpoint shows which endpoint this member is; either {@code SOURCE} or {@code DESTINATION} </li>
 * <li>Current replica index denotes the replica index which is currently owned by this member before migration.</li>
 * <li>New replica index denotes the replica index which will be owned by this member after migration.</li>
 * </ul>
 * <p>
 * On migration source, partition replica will be either removed completely or shifted down to a higher indexed replica.
 * During commit, service should remove either all or some part of its data in this partition
 * and run any other action needed. A sample commit on source will look like;
 * <pre>
 * public void commitMigration(PartitionMigrationEvent event) {
 *     if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
 *         if (event.getNewReplicaIndex() == -1 || event.getNewReplicaIndex() &gt; configuredBackupCount) {
 *             // remove data...
 *         }
 *     }
 *     // run any other task needed
 * }
 * </pre>
 * During rollback, source usually doesn't expected to perform any task. But service implementation may need
 * to execute custom tasks.
 * <p>
 * On migration destination, either a fresh partition replica will be received or current replica will be
 * shifted up to a lower indexed replica.
 * During rollback, service should remove either all or some part of its data in this partition
 * and run any other action needed. A sample rollback on destination will look like;
 * <pre>
 * <code>
 * public void rollbackMigration(PartitionMigrationEvent event) {
 *      if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
 *          if (event.getCurrentReplicaIndex() == -1 || event.getCurrentReplicaIndex() &gt; configuredBackupCount) {
 *              // remove data...
 *          }
 *      }
 *     // run any other task needed
 * }
 * </code>
 * </pre>
 * <p>
 * During commit, destination usually doesn't expected to perform any task. But service implementation may need
 * to execute custom tasks.
 */
public interface MigrationAwareService {

    /**
     * Returns an operation to replicate service data and/or state for a specific partition replica
     * on another cluster member.
     * <p>
     * This method will be called on source member whenever partitioning system requires
     * to copy/replicate a partition replica. Returned operation will be executed on destination member.
     * If operation fails by throwing exception, migration process will fail and will be rolled back.
     * <p>
     * Returning null is allowed and means service does not have anything to replicate.
     *
     * @param event replication
     * @return replication operation or null if nothing will be replicated
     */
    Operation prepareReplicationOperation(PartitionReplicationEvent event);

    /**
     * Called before migration process starts, on both source and destination members.
     * <p>
     * Service can take actions required before migration. Migration process will block until this method returns.
     * If this method fails by throwing an exception, migration process for specific partition will fail
     * and will be rolled back.
     *
     * @param event migration event
     */
    void beforeMigration(PartitionMigrationEvent event);

    /**
     * Commits the migration process for this service, on both source and destination members.
     * This method will be called after all replication operations are executed successfully on destination
     * and master member receives success response from all participants.
     * <p>
     * Commit is not expected to fail at this point, all exceptions will be suppressed and logged.
     * Implementations of this method must be thread safe as this method may be called concurrently
     * for different migrations on different partitions.
     *
     * @param event migration event
     */
    void commitMigration(PartitionMigrationEvent event);

    /**
     * Rollback the migration process for this service, on both source and destination members.
     * This method will be called when migration process fails at any moment.
     * Reasons for failure may be an exception thrown on any migration step
     * or failure(s) of any of the migration participants; either master or source or destination.
     * <p>
     * Rollback is not expected to fail at this point, all exceptions will be suppressed and logged.
     * Implementations of this method must be thread safe as this method may be called concurrently
     * for different migrations on different partitions.
     *
     * @param event migration event
     */
    void rollbackMigration(PartitionMigrationEvent event);
}
