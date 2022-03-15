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

/**
 * MigrationState shows statistical information about the migration,
 * such as; migration start time, number of planned replica migrations,
 * number of completed replica migrations, total elapsed migration time etc.
 *
 * @see MigrationListener
 * @see ReplicaMigrationEvent
 */
public interface MigrationState {
    /**
     * Returns the start time of the migration in milliseconds since the epoch.
     *
     * @return start time of the migration process
     */
    long getStartTime();

    /**
     * Returns the number of planned migrations in the migration plan.
     *
     * @return number of planned migrations
     */
    int getPlannedMigrations();

    /**
     * Returns the number of completed migrations in the migration plan.
     *
     * @return number of completed migrations
     */
    int getCompletedMigrations();

    /**
     * Returns the number of remaining migrations in the migration plan.
     *
     * @return number of remaining migrations
     */
    int getRemainingMigrations();

    /**
     * Returns the total elapsed time of completed migrations in milliseconds.
     *
     * @return total elapsed time in milliseconds
     */
    long getTotalElapsedTime();

}
