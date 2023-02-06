/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.services.ServiceNamespace;

import java.util.Arrays;

import static com.hazelcast.internal.partition.IPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.internal.partition.impl.PartitionReplicaManager.REQUIRES_SYNC;
import static java.lang.System.arraycopy;

// read and updated only by partition threads
final class PartitionReplicaFragmentVersions {
    private final int partitionId;
    private final ServiceNamespace namespace;
    private final long[] versions = new long[MAX_BACKUP_COUNT];
    /**
     * Shows whether partition has missing backups somewhere between the last applied backup
     * and the last incremental backup received.
     */
    private boolean dirty;

    PartitionReplicaFragmentVersions(int partitionId, ServiceNamespace namespace) {
        this.partitionId = partitionId;
        this.namespace = namespace;
    }

    /**
     * Increments partition replica versions on partition owner
     * when backup operation is prepared and sent to replica.
     *
     * If a replica is designated as requiring sync,
     * do not increment because a sync has not occurred yet.
     * It will be reset to 0 when
     * {@link PartitionReplicaManager#getPartitionReplicaVersionsForSync}
     * is executed.
     */
    long[] incrementAndGet(int backupCount) {
        for (int i = 0; i < backupCount; i++) {
            if (versions[i] != REQUIRES_SYNC) {
                versions[i]++;
            }
        }
        return versions;
    }

    long[] get() {
        return versions;
    }

    /**
     * Returns whether given replica version is behind the current version or not.
     * @param newVersions new replica versions
     * @param replicaIndex replica index
     * @return true if given version is stale, false otherwise
     */
    boolean isStale(long[] newVersions, int replicaIndex) {
        int index = replicaIndex - 1;
        long currentVersion = versions[index];
        long newVersion = newVersions[index];
        return currentVersion > newVersion || currentVersion == REQUIRES_SYNC;
    }

    /**
     * Updates replica version if it is newer than current version. Otherwise, has no effect.
     * Marks versions as dirty if version increase is not incremental.
     * Executed on backup replica owner.
     *
     * @param newVersions new replica versions
     * @param replicaIndex replica index
     * @return returns false if versions are dirty, true otherwise
     */
    boolean update(long[] newVersions, int replicaIndex) {
        int index = replicaIndex - 1;
        long currentVersion = versions[index];
        long nextVersion = newVersions[index];

        if (currentVersion == REQUIRES_SYNC) {
            // the replica is marked explicitly for partition sync,
            // so maintain it as is and mark versions as dirty.
            dirty = true;
            return true;
        }

        if (currentVersion < nextVersion) {
            setVersions(newVersions, replicaIndex);
            dirty = dirty || (nextVersion - currentVersion > 1);
        }
        return !dirty;
    }

    /** Change versions for all replicas with an index greater than {@code fromReplica} to the new replica versions */
    private void setVersions(long[] newVersions, int fromReplica) {
        int fromIndex = fromReplica - 1;
        int len = newVersions.length - fromIndex;
        arraycopy(newVersions, fromIndex, versions, fromIndex, len);
    }

    void set(long[] newVersions, int fromReplica) {
        setVersions(newVersions, fromReplica);
        dirty = false;
    }

    boolean isDirty() {
        return dirty;
    }

    void markAsSyncRequired(int replicaIndex) {
        versions[replicaIndex - 1] = REQUIRES_SYNC;
    }

    void clear() {
        Arrays.fill(versions, 0);
        dirty = false;
    }

    @Override
    public String toString() {
        return "PartitionReplicaFragmentVersions{" + "partitionId=" + partitionId + ", namespace=" + namespace
                + ", versions=" + Arrays.toString(versions) + ", dirty=" + dirty + '}';
    }
}
