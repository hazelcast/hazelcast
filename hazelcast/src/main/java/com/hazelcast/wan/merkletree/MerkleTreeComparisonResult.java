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
 * Result of the last WAN merkle tree comparison
 */
public class MerkleTreeComparisonResult {
    private final int lastCheckedCount;
    private final int lastDiffCount;

    public MerkleTreeComparisonResult() {
        lastCheckedCount = 0;
        lastDiffCount = 0;
    }

    /**
     * Constructs the result of the merkle tree root comparison
     *
     * @param lastCheckedCount the number of last checked objects
     * @param lastDiffCount    the number of different objects
     */
    public MerkleTreeComparisonResult(int lastCheckedCount,
                                      int lastDiffCount) {
        this.lastCheckedCount = lastCheckedCount;
        this.lastDiffCount = lastDiffCount;
    }

    public int getLastCheckedCount() {
        return lastCheckedCount;
    }

    public int getLastDiffCount() {
        return lastDiffCount;
    }
}
