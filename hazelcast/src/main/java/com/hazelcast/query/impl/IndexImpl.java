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

package com.hazelcast.query.impl;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.query.impl.getters.Extractors;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.newSetFromMap;

/**
 * Provides implementation of on-heap indexes.
 */
public class IndexImpl extends AbstractIndex {

    private final Set<Integer> indexedPartitions = newSetFromMap(new ConcurrentHashMap<>());

    public IndexImpl(
        IndexConfig config,
        InternalSerializationService ss,
        Extractors extractors,
        IndexCopyBehavior copyBehavior,
        PerIndexStats stats
    ) {
        super(config, ss, extractors, copyBehavior, stats, null);
    }

    @Override
    protected IndexStore createIndexStore(boolean ordered, PerIndexStats stats) {
        return ordered ? new OrderedIndexStore(copyBehavior) : new UnorderedIndexStore(copyBehavior);
    }

    @Override
    public void clear() {
        super.clear();
        indexedPartitions.clear();
    }

    @Override
    public boolean hasPartitionIndexed(int partitionId) {
        return indexedPartitions.contains(partitionId);
    }

    @Override
    public void markPartitionAsIndexed(int partitionId) {
        assert !indexedPartitions.contains(partitionId);
        indexedPartitions.add(partitionId);
    }

    @Override
    public void markPartitionAsUnindexed(int partitionId) {
        indexedPartitions.remove(partitionId);
    }

}
