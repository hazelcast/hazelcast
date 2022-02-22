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

/**
 * An event fired when a partition replica migration completes or fails.
 *
 * @see Partition
 * @see PartitionService
 * @see MigrationListener
 */
public interface ReplicaMigrationEvent extends PartitionEvent {

    /**
     * Returns the index of the partition replica.
     * 0th index is primary replica, 1st index is 1st backup and so on.
     *
     * @return the index of the partition replica
     */
    int getReplicaIndex();

    /**
     * Returns the old owner of the migrating partition replica.
     *
     * @return the old owner of the migrating partition replica
     */
    Member getSource();

    /**
     * Returns the new owner of the migrating partition replica.
     *
     * @return the new owner of the migrating partition replica
     */
    Member getDestination();

    /**
     * Returns the result of the migration: completed or failed.
     *
     * @return true if the migration completed successfully, false otherwise
     */
    boolean isSuccess();

    /**
     * Returns the elapsed the time of this migration in milliseconds.
     *
     * @return elapsed time in milliseconds.
     */
    long getElapsedTime();

    /**
     * Returns the progress information of the overall migration.
     *
     * @return migration process progress
     */
    MigrationState getMigrationState();
}
