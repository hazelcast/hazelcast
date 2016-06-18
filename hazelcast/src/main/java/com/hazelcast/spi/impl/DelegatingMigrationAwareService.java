/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link MigrationAwareService} that delegates to another {@link MigrationAwareService} and keeps track of the number of
 * migrations concerning the partition owner (either as current or new replica index) currently in-flight.
 */
public class DelegatingMigrationAwareService implements MigrationAwareService {

    private static final int PARTITION_OWNER_INDEX = 0;

    private final MigrationAwareService migrationAwareService;
    // number of currently executing migrations on the partition owner
    private final AtomicInteger ownerMigrationsInFlight;

    public DelegatingMigrationAwareService(MigrationAwareService migrationAwareService) {
        this.migrationAwareService = migrationAwareService;
        this.ownerMigrationsInFlight = new AtomicInteger();
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return migrationAwareService.prepareReplicationOperation(event);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        if (event.getCurrentReplicaIndex() == PARTITION_OWNER_INDEX || event.getNewReplicaIndex() == PARTITION_OWNER_INDEX) {
            ownerMigrationsInFlight.incrementAndGet();
        }
        migrationAwareService.beforeMigration(event);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        try {
            migrationAwareService.commitMigration(event);
        } finally {
            if (event.getCurrentReplicaIndex() == PARTITION_OWNER_INDEX || event.getNewReplicaIndex() == PARTITION_OWNER_INDEX) {
                int count = ownerMigrationsInFlight.decrementAndGet();
                assert count >= 0;
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        try {
            migrationAwareService.rollbackMigration(event);
        } finally {
            if (event.getCurrentReplicaIndex() == PARTITION_OWNER_INDEX || event.getNewReplicaIndex() == PARTITION_OWNER_INDEX) {
                int count = ownerMigrationsInFlight.decrementAndGet();
                assert count >= 0;
            }
        }
    }

    /**
     * Get the number of currently executing migrations. This is actually
     * (count of beforeMigration) - (count of rollbackMigration + count of commitMigration)
     * @return the number of migrations which are currently processed.
     */
    public int getOwnerMigrationsInFlight() {
        return ownerMigrationsInFlight.get();
    }
}
