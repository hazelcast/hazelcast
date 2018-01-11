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

package com.hazelcast.map.impl.query;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntriesSegment;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.exception.RetryableHazelcastException;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Implementation of the {@link PartitionScanExecutor} which executes the partition scan in a sequential-fashion
 * in the calling thread.
 */
public class CallerRunsPartitionScanExecutor implements PartitionScanExecutor {

    private final PartitionScanRunner partitionScanRunner;

    public CallerRunsPartitionScanExecutor(PartitionScanRunner partitionScanRunner) {
        this.partitionScanRunner = partitionScanRunner;
    }

    @Override
    public Collection<QueryableEntry> execute(String mapName, Predicate predicate, Collection<Integer> partitions) {
        RetryableHazelcastException storedException = null;
        Collection<QueryableEntry> result = new ArrayList<QueryableEntry>();
        for (Integer partitionId : partitions) {
            try {
                result.addAll(partitionScanRunner.run(mapName, predicate, partitionId));
            } catch (RetryableHazelcastException e) {
                // RetryableHazelcastException are stored and re-thrown later. this is to ensure all partitions
                // are touched as when the parallel execution was used.
                // see discussion at https://github.com/hazelcast/hazelcast/pull/5049#discussion_r28773099 for details.
                if (storedException == null) {
                    storedException = e;
                }
            }
        }
        if (storedException != null) {
            throw storedException;
        }
        return result;
    }

    @Override
    public QueryableEntriesSegment execute(String mapName, Predicate predicate, int partitionId, int tableIndex, int fetchSize) {
        return partitionScanRunner.run(mapName, predicate, partitionId, tableIndex, fetchSize);
    }
}
