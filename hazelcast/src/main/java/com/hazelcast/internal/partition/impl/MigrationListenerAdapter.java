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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.PartitionEventListener;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.partition.MigrationListener;

/**
 * Wraps a migration listener and dispatches a migration event to matching method
 */
class MigrationListenerAdapter implements PartitionEventListener<ReplicaMigrationEvent> {

    static final int MIGRATION_STARTED_PARTITION_ID = -1;
    static final int MIGRATION_FINISHED_PARTITION_ID = -2;

    private final MigrationListener migrationListener;

    MigrationListenerAdapter(MigrationListener migrationListener) {
        this.migrationListener = migrationListener;
    }

    @Override
    public void onEvent(ReplicaMigrationEvent event) {
        switch (event.getPartitionId()) {
            case MIGRATION_STARTED_PARTITION_ID:
                migrationListener.migrationStarted(event.getMigrationState());
                break;
            case MIGRATION_FINISHED_PARTITION_ID:
                migrationListener.migrationFinished(event.getMigrationState());
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
