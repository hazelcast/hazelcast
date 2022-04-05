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

import java.util.EventListener;

/**
 * MigrationListener provides the ability to listen to
 * partition migration process and events.
 *
 * @see Partition
 * @see PartitionService
 * @see MigrationState
 * @see ReplicaMigrationEvent
 */
public interface MigrationListener extends EventListener {

    /**
     * Called when the migration process starts.
     * A migration process consists of a group of partition
     * replica migrations which are planned together.
     * <p>
     * When migration process is completed, {@link #migrationFinished(MigrationState)}
     * is called.
     *
     * @param state Plan of the migration process
     */
    void migrationStarted(MigrationState state);

    /**
     * Called when the migration process finishes.
     * This event denotes ending of migration process which is
     * started by {@link #migrationStarted(MigrationState)}.
     * <p>
     * Not all of the planned migrations have to be completed.
     * Some of them can be skipped because of a newly created migration plan.
     * <p>
     * If migration process coordinator member (generally the oldest member in cluster)
     * crashes before migration process ends, then this method may not be called at all.
     *
     * @param state Result of the migration process
     */
    void migrationFinished(MigrationState state);

    /**
     * Called when a partition replica migration is completed successfully.
     *
     * @param event the event for the partition replica migration
     */
    void replicaMigrationCompleted(ReplicaMigrationEvent event);

    /**
     * Called when a partition replica migration is failed.
     *
     * @param event the event for the partition replica migration
     */
    void replicaMigrationFailed(ReplicaMigrationEvent event);

}
