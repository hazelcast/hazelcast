/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan;

import com.hazelcast.internal.partition.PartitionMigrationEvent;

/**
 * Interface for WAN publisher migration related events. Can be implemented
 * by WAN publishers to listen to migration events, for example to maintain
 * the WAN event counters.
 * <p>
 * None of the methods of this interface is expected to block or fail.
 * NOTE: used only in Hazelcast Enterprise.
 *
 * @see PartitionMigrationEvent
 * @see com.hazelcast.internal.partition.MigrationAwareService
 */
public interface WanReplicationPublisherMigrationListener {
    /**
     * Indicates that migration started for a given partition
     *
     * @param event the migration event
     */
    void onMigrationStart(PartitionMigrationEvent event);

    /**
     * Indicates that migration is committing for a given partition
     *
     * @param event the migration event
     */
    void onMigrationCommit(PartitionMigrationEvent event);

    /**
     * Indicates that migration is rolling back for a given partition
     *
     * @param event the migration event
     */
    void onMigrationRollback(PartitionMigrationEvent event);
}
