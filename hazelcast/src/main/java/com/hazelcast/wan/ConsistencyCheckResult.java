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

import java.util.UUID;

/**
 * Result of the last WAN consistency check result.
 */
public class ConsistencyCheckResult {
    /**
     * The UUID of the consistency check request.
     */
    private final UUID uuid;
    /**
     * Number of checked partitions.
     */
    private final int lastCheckedPartitionCount;
    /**
     * Number of partitions found to be inconsistent.
     */
    private final int lastDiffPartitionCount;
    /**
     * Number of checked Merkle tree leaves.
     */
    private final int lastCheckedLeafCount;
    /**
     * Number of different Merkle tree leaves.
     */
    private final int lastDiffLeafCount;
    /**
     * Number of entries to synchronize to get the clusters into sync.
     */
    private final int lastEntriesToSync;

    public ConsistencyCheckResult(UUID uuid) {
        this.uuid = uuid;
        lastCheckedPartitionCount = 0;
        lastDiffPartitionCount = 0;
        lastCheckedLeafCount = 0;
        lastDiffLeafCount = 0;
        lastEntriesToSync = 0;
    }

    /**
     * Constructs the result of the WAN consistency check comparison
     * @param uuid                      the UUID of the consistency check
     *                                  request
     * @param lastCheckedPartitionCount the number of last checked
     *                                  partitions
     * @param lastDiffPartitionCount    the number of different partitions
     * @param lastCheckedLeafCount      the number of the checked Merkle
     *                                  tree leaves
     * @param lastDiffLeafCount         the number of the Merkle trees
     *                                  found different
     * @param lastEntriesToSync         the number of the entries need to
     */
    public ConsistencyCheckResult(UUID uuid, int lastCheckedPartitionCount, int lastDiffPartitionCount, int lastCheckedLeafCount,
                                  int lastDiffLeafCount, int lastEntriesToSync) {
        this.uuid = uuid;
        this.lastCheckedPartitionCount = lastCheckedPartitionCount;
        this.lastDiffPartitionCount = lastDiffPartitionCount;
        this.lastCheckedLeafCount = lastCheckedLeafCount;
        this.lastDiffLeafCount = lastDiffLeafCount;
        this.lastEntriesToSync = lastEntriesToSync;
    }

    public UUID getUuid() {
        return uuid;
    }

    public int getLastCheckedPartitionCount() {
        return lastCheckedPartitionCount;
    }

    public int getLastDiffPartitionCount() {
        return lastDiffPartitionCount;
    }

    public int getLastCheckedLeafCount() {
        return lastCheckedLeafCount;
    }

    public int getLastDiffLeafCount() {
        return lastDiffLeafCount;
    }

    public int getLastEntriesToSync() {
        return lastEntriesToSync;
    }

    public boolean isRunning() {
        return lastCheckedPartitionCount == -1 && lastDiffPartitionCount == -1 && lastCheckedLeafCount == -1
                && lastDiffLeafCount == -1 && lastEntriesToSync == -1;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public float getDiffPercentage() {
        return lastCheckedPartitionCount != 0
                ? (float) lastDiffLeafCount / lastCheckedLeafCount * 100
                : 0;
    }

    public boolean isDone() {
        return lastCheckedPartitionCount > 0 && lastDiffPartitionCount >= 0 && lastCheckedLeafCount > 0
                && lastDiffLeafCount >= 0 && lastEntriesToSync >= 0;
    }
}
