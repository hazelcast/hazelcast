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

import java.util.UUID;

/**
 * An {@link java.util.EventObject} for a partition migration. Can be used by SPI services to get a callback
 * to listen to partition migration.  See {@link MigrationAwareService} for more info.
 */
public class PartitionMigrationEvent {

    private final MigrationEndpoint migrationEndpoint;

    private final int partitionId;

    private final int currentReplicaIndex;

    private final int newReplicaIndex;

    private final UUID migrationUid;

    public PartitionMigrationEvent(MigrationEndpoint migrationEndpoint, int partitionId, int currentReplicaIndex,
            int newReplicaIndex, UUID migrationUid) {
        this.migrationEndpoint = migrationEndpoint;
        this.partitionId = partitionId;
        this.currentReplicaIndex = currentReplicaIndex;
        this.newReplicaIndex = newReplicaIndex;
        this.migrationUid = migrationUid;
    }

    /**
     * Gets the partition migration endpoint.
     *
     * @return the partition migration endpoint
     */
    public MigrationEndpoint getMigrationEndpoint() {
        return migrationEndpoint;
    }

    /**
     * Gets the partition ID.
     *
     * @return the partition ID
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Gets the index of the partition replica that current member owns currently, before migration starts.
     * This index will be in range of [0,6] if current member owns a replica of the partition. Otherwise it will be -1.
     *
     * @return index of the partition replica that current member owns currently
     */
    public int getCurrentReplicaIndex() {
        return currentReplicaIndex;
    }

    /**
     * Gets the index of the partition replica that current member will own after migration is committed.
     * This index will be -1 if partition replica will be moved from current member completely.
     *
     * @return index of the partition replica that current member will own after migration is committed
     */
    public int getNewReplicaIndex() {
        return newReplicaIndex;
    }

    /**
     * Returns the uid of the specific migration.
     */
    public UUID getMigrationUid() {
        return migrationUid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionMigrationEvent that = (PartitionMigrationEvent) o;

        if (partitionId != that.partitionId) {
            return false;
        }
        if (currentReplicaIndex != that.currentReplicaIndex) {
            return false;
        }
        if (newReplicaIndex != that.newReplicaIndex) {
            return false;
        }
        return migrationEndpoint == that.migrationEndpoint;

    }

    @Override
    public int hashCode() {
        int result = migrationEndpoint.hashCode();
        result = 31 * result + partitionId;
        result = 31 * result + currentReplicaIndex;
        result = 31 * result + newReplicaIndex;
        return result;
    }

    @Override
    public String toString() {
        return "PartitionMigrationEvent{"
                + "migrationEndpoint=" + migrationEndpoint
                + ", partitionId=" + partitionId
                + ", currentReplicaIndex=" + currentReplicaIndex
                + ", newReplicaIndex=" + newReplicaIndex
                + ", uuid=" + migrationUid
                + '}';
    }
}
