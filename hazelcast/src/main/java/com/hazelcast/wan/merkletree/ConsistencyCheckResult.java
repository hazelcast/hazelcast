/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.merkletree;

/**
 * Result of the last WAN consistency check result
 */
public class ConsistencyCheckResult {
    /**
     * Number of checked partitions
     */
    private final int lastCheckedPartitionCount;
    /**
     * Number of partitions found to be inconsistent
     */
    private final int lastDiffPartitionCount;

    public ConsistencyCheckResult() {
        lastCheckedPartitionCount = 0;
        lastDiffPartitionCount = 0;
    }

    /**
     * Constructs the result of the WAN consistency check comparison
     *
     * @param lastCheckedPartitionCount the number of last checked partitions
     * @param lastDiffPartitionCount    the number of different partitions
     */
    public ConsistencyCheckResult(int lastCheckedPartitionCount, int lastDiffPartitionCount) {
        this.lastCheckedPartitionCount = lastCheckedPartitionCount;
        this.lastDiffPartitionCount = lastDiffPartitionCount;
    }

    public int getLastCheckedPartitionCount() {
        return lastCheckedPartitionCount;
    }

    public int getLastDiffPartitionCount() {
        return lastDiffPartitionCount;
    }

    public boolean isRunning() {
        return lastCheckedPartitionCount == -1 && lastDiffPartitionCount == -1;
    }

    @SuppressWarnings("checkstyle:magicnumber")
    public float getDiffPercentage() {
        return lastCheckedPartitionCount != 0
                ? (float) lastDiffPartitionCount / lastCheckedPartitionCount * 100
                : 0;
    }

    public boolean isDone() {
        return lastCheckedPartitionCount > 0 && lastDiffPartitionCount > 0;
    }
}
