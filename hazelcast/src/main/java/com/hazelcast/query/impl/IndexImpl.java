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

package com.hazelcast.query.impl;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.impl.GlobalIndexPartitionTracker.PartitionStamp;
import com.hazelcast.query.impl.getters.Extractors;

/**
 * Provides implementation of on-heap indexes.
 */
public class IndexImpl extends AbstractIndex {

    private final GlobalIndexPartitionTracker partitionTracker;

    public IndexImpl(
            IndexConfig config,
            InternalSerializationService ss,
            Extractors extractors,
            IndexCopyBehavior copyBehavior,
            PerIndexStats stats,
            int partitionCount
    ) {
        super(config, ss, extractors, copyBehavior, stats);

        partitionTracker = new GlobalIndexPartitionTracker(partitionCount);
    }

    @Override
    protected IndexStore createIndexStore(IndexConfig config, PerIndexStats stats) {
        switch (config.getType()) {
            case SORTED:
                return new OrderedIndexStore(copyBehavior);
            case HASH:
                return new UnorderedIndexStore(copyBehavior);
            case BITMAP:
                return new BitmapIndexStore(config);
            default:
                throw new IllegalArgumentException("unexpected index type: " + config.getType());
        }
    }

    @Override
    public final boolean hasPartitionIndexed(int partitionId) {
        return partitionTracker.isIndexed(partitionId);
    }

    @Override
    public final boolean allPartitionsIndexed(int ownedPartitionCount) {
        // This check guarantees that all partitions are indexed
        // only if there is no concurrent migrations. Check migration stamp
        // to detect concurrent migrations if needed.
        return ownedPartitionCount < 0 || partitionTracker.indexedCount() == ownedPartitionCount;
    }

    @Override
    public final void beginPartitionUpdate() {
        partitionTracker.beginPartitionUpdate();
    }

    @Override
    public final void markPartitionAsIndexed(int partitionId) {
        partitionTracker.partitionIndexed(partitionId);
    }

    @Override
    public final void markPartitionAsUnindexed(int partitionId) {
        partitionTracker.partitionUnindexed(partitionId);
    }

    @Override
    public final void clear() {
        partitionTracker.clear();

        super.clear();
    }

    @Override
    public final PartitionStamp getPartitionStamp() {
        return partitionTracker.getPartitionStamp();
    }

    @Override
    public final boolean validatePartitionStamp(long stamp) {
        return partitionTracker.validatePartitionStamp(stamp);
    }

    @Override
    public String toString() {
        return "IndexImpl{"
                + "partitionTracker=" + partitionTracker
                + "} " + super.toString();
    }
}
