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

package com.hazelcast.query.impl;

import com.hazelcast.monitor.impl.PerIndexStats;

/**
 * Provides the private index API.
 */
public interface InternalIndex extends Index {

    /**
     * Returns {@code true} if the given partition is indexed by this index,
     * {@code false} otherwise.
     */
    boolean hasPartitionIndexed(int partitionId);

    /**
     * Marks the given partition as indexed by this index.
     *
     * @param partitionId the ID of the partition to mark as indexed.
     */
    void markPartitionAsIndexed(int partitionId);

    /**
     * Marks the given partition as unindexed by this index.
     *
     * @param partitionId the ID of the partition to mark as unindexed.
     */
    void markPartitionAsUnindexed(int partitionId);

    /**
     * Returns the index stats associated with this index.
     */
    PerIndexStats getPerIndexStats();

}
