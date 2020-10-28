/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;

public final class MigrationEventHandler {

    public static final int MIGRATION_STARTED = -1;
    public static final int MIGRATION_FINISHED = -2;

    private final MigrationListener migrationListener;

    public MigrationEventHandler(MigrationListener migrationListener) {
        this.migrationListener = migrationListener;
    }

    public void handleMigrationState(MigrationState state, int partitionId) {
        switch (partitionId) {
            case MIGRATION_STARTED:
                migrationListener.migrationStarted(state);
                break;
            case MIGRATION_FINISHED:
                migrationListener.migrationFinished(state);
                break;
            default:
                break;
        }
    }

    public void handleReplicaMigration(ReplicaMigrationEvent event) {
        switch (event.getPartitionId()) {
            case MIGRATION_STARTED:
            case MIGRATION_FINISHED:
                handleMigrationState(event.getMigrationState(), event.getPartitionId());
                break;
            default:
                if (event.isSuccess()) {
                    migrationListener.replicaMigrationCompleted(event);
                } else {
                    migrationListener.replicaMigrationFailed(event);
                }
        }
    }
}
