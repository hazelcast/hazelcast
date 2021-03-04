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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.HashUtil;
import com.hazelcast.internal.util.IterableUtil;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntriesSegment;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Collection;

import static com.hazelcast.internal.util.SetUtil.singletonPartitionIdSet;

/**
 * Runs query operations in the calling thread (thus blocking it)
 * <p>
 * Used by query operations only: QueryOperation &amp; QueryPartitionOperation
 * Should not be used by proxies or any other query related objects.
 */
public class QueryRunner {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final QueryResultSizeLimiter queryResultSizeLimiter;
    protected final InternalSerializationService serializationService;
    protected final QueryOptimizer queryOptimizer;
    protected final OperationService operationService;
    protected final ClusterService clusterService;
    protected final LocalMapStatsProvider localMapStatsProvider;
    protected final PartitionScanExecutor partitionScanExecutor;
    protected final ResultProcessorRegistry resultProcessorRegistry;

    private final int partitionCount;

    public QueryRunner(MapServiceContext mapServiceContext,
                       QueryOptimizer optimizer,
                       PartitionScanExecutor partitionScanExecutor,
                       ResultProcessorRegistry resultProcessorRegistry) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.logger = nodeEngine.getLogger(getClass());
        this.queryResultSizeLimiter = new QueryResultSizeLimiter(mapServiceContext, logger);
        this.queryOptimizer = optimizer;
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
        this.localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
        this.partitionScanExecutor = partitionScanExecutor;
        this.resultProcessorRegistry = resultProcessorRegistry;
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
    }

    /**
     * Runs a query on a chunk of a single partition. The chunk is defined by
     * the {@code pointers} and the soft limit is defined by the {@code fetchSize}.
     *
     * @param query       the query
     * @param partitionId the partition which is queried
     * @param pointers    the pointers defining the state of iteration
     * @param fetchSize   the soft limit for the number of items to be queried
     * @return the queried entries along with the next {@code tableIndex} to resume querying
     */
    public ResultSegment runPartitionScanQueryOnPartitionChunk(Query query,
                                                               int partitionId,
                                                               IterationPointer[] pointers,
                                                               int fetchSize) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(query.getMapName());
        Predicate predicate = queryOptimizer.optimize(query.getPredicate(), mapContainer.getIndexes(partitionId));
        QueryableEntriesSegment entries = partitionScanExecutor
                .execute(query.getMapName(), predicate, partitionId, pointers, fetchSize);

        ResultProcessor processor = resultProcessorRegistry.get(query.getResultType());
        Result result = processor.populateResult(query, Long.MAX_VALUE, entries.getEntries(),
                singletonPartitionIdSet(partitionCount, partitionId));

        return new ResultSegment(result, entries.getPointers());
    }


    public Result runIndexOrPartitionScanQueryOnOwnedPartitions(Query query) {
        Result result = runIndexOrPartitionScanQueryOnOwnedPartitions(query, true);
        assert result != null;
        return result;
    }

    /**
     * MIGRATION SAFE QUERYING -> MIGRATION STAMPS ARE VALIDATED (does not have to run on a partition thread)
     * full query = index query (if possible), then partition-scan query
     *
     * @param query           the query to execute
     * @param doPartitionScan whether to run full scan ion partitions if the global index run failed.
     * @return the query result. {@code null} if the {@code doPartitionScan} is set and the execution on the
     * global index failed.
     */
    public Result runIndexOrPartitionScanQueryOnOwnedPartitions(Query query, boolean doPartitionScan) {
        int migrationStamp = getMigrationStamp();
        PartitionIdSet initialPartitions = mapServiceContext.getOrInitCachedMemberPartitions();
        PartitionIdSet actualPartitions = query.getPartitionIdSet() != null
                ? initialPartitions.intersectCopy(query.getPartitionIdSet())
                : initialPartitions;

        MapContainer mapContainer = mapServiceContext.getMapContainer(query.getMapName());

        // to optimize the query we need to get any index instance
        Indexes indexes = mapContainer.getIndexes();
        if (indexes == null) {
            indexes = mapContainer.getIndexes(initialPartitions.iterator().next());
        }
        // first we optimize the query
        Predicate predicate = queryOptimizer.optimize(query.getPredicate(), indexes);

        // then we try to run using an index, but if that doesn't work, we'll try a full table scan
        Iterable<QueryableEntry> entries = runUsingGlobalIndexSafely(predicate, mapContainer,
                migrationStamp, initialPartitions.size());

        if (entries != null && !initialPartitions.equals(actualPartitions)) {
            assert indexes.isGlobal();
            // if the query runs on a subset of partitions, filter the results from a global index
            entries = IterableUtil.filter(entries,
                    e -> {
                        int partitionId = HashUtil.hashToIndex(e.getKeyData().getPartitionHash(), partitionCount);
                        return actualPartitions.contains(partitionId);
                    });
        }

        if (entries == null && !doPartitionScan) {
            return null;
        }

        Result result;
        if (entries == null) {
            result = runUsingPartitionScanSafely(query, predicate, actualPartitions, migrationStamp);
            if (result == null) {
                // full scan didn't work, returning empty result
                result = populateEmptyResult(query, actualPartitions);
            }
        } else {
            result = populateNonEmptyResult(query, entries, actualPartitions);
        }

        return result;
    }

    /**
     * Performs the given query using indexes.
     * <p>
     * The method may return a special failure result, which has {@code null}
     * {@link Result#getPartitionIds() partition IDs}, in the following
     * situations:
     * <ul>
     * <li>If a partition migration is detected during the query execution.
     * <li>If it's impossible to perform the given query using indexes.
     * </ul>
     * <p>
     * The method may be invoked on any thread.
     *
     * @param query the query to perform.
     * @return the result of the query; if the result has {@code null} {@link
     * Result#getPartitionIds() partition IDs} this indicates a failure.
     */
    public Result runIndexQueryOnOwnedPartitions(Query query) {
        int migrationStamp = getMigrationStamp();
        PartitionIdSet initialPartitions = mapServiceContext.getOrInitCachedMemberPartitions();
        MapContainer mapContainer = mapServiceContext.getMapContainer(query.getMapName());

        // to optimize the query we need to get any index instance
        Indexes indexes = mapContainer.getIndexes();
        if (indexes == null) {
            indexes = mapContainer.getIndexes(initialPartitions.iterator().next());
        }
        // first we optimize the query
        Predicate predicate = queryOptimizer.optimize(query.getPredicate(), indexes);

        // then we try to run using an index
        Iterable<QueryableEntry> entries = runUsingGlobalIndexSafely(predicate, mapContainer,
                migrationStamp, initialPartitions.size());

        Result result;
        if (entries == null) {
            // failed with index query because of ongoing migrations
            result = populateEmptyResult(query, initialPartitions);
        } else {
            // success
            result = populateNonEmptyResult(query, entries, initialPartitions);
        }

        return result;
    }

    // MIGRATION UNSAFE QUERYING - MIGRATION STAMPS ARE NOT VALIDATED, so assumes a run on partition-thread
    // for a single partition. If the index is global it won't be asked
    public Result runPartitionIndexOrPartitionScanQueryOnGivenOwnedPartition(Query query, int partitionId) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(query.getMapName());
        PartitionIdSet partitions = singletonPartitionIdSet(partitionCount, partitionId);

        // first we optimize the query
        Predicate predicate = queryOptimizer.optimize(query.getPredicate(), mapContainer.getIndexes(partitionId));

        Iterable<QueryableEntry> entries = null;
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (indexes != null && !indexes.isGlobal()) {
            entries = indexes.query(predicate, partitions.size());
        }

        Result result;
        if (entries == null) {
            result = createResult(query, partitions);
            partitionScanExecutor.execute(query.getMapName(), predicate, partitions, result);
            result.completeConstruction(partitions);
        } else {
            result = populateNonEmptyResult(query, entries, partitions);
        }

        return result;
    }

    private Result createResult(Query query, Collection<Integer> partitions) {
        return query.createResult(serializationService, queryResultSizeLimiter.getNodeResultLimit(partitions.size()));
    }

    protected Result populateEmptyResult(Query query, Collection<Integer> initialPartitions) {
        return resultProcessorRegistry.get(query.getResultType())
                .populateResult(query, queryResultSizeLimiter.getNodeResultLimit(initialPartitions.size()));
    }

    protected Result populateNonEmptyResult(Query query, Iterable<QueryableEntry> entries,
                                            PartitionIdSet initialPartitions) {
        ResultProcessor processor = resultProcessorRegistry.get(query.getResultType());
        return processor.populateResult(query, queryResultSizeLimiter.getNodeResultLimit(initialPartitions.size()), entries,
                initialPartitions);
    }

    protected Iterable<QueryableEntry> runUsingGlobalIndexSafely(Predicate predicate, MapContainer mapContainer,
                                                                 int migrationStamp, int ownedPartitionCount) {

        // If a migration is in progress or migration ownership changes,
        // do not attempt to use an index as they may have not been created yet.
        if (!validateMigrationStamp(migrationStamp)) {
            return null;
        }

        Indexes indexes = mapContainer.getIndexes();
        if (indexes == null) {
            return null;
        }
        if (!indexes.isGlobal()) {
            // rolling-upgrade compatibility guide, if the index is not global we can't use it in the global scan.
            // it may happen if the 3.9 EE node receives a QueryOperation from a 3.8 node, in this case we can't
            // leverage index on this node in a global way.
            return null;
        }
        Iterable<QueryableEntry> entries = indexes.query(predicate, ownedPartitionCount);
        if (entries == null) {
            return null;
        }

        // If a migration is in progress or migration ownership changes,
        // do not attempt to use an index as they may have not been created yet.
        // This means migrations were executed and we may
        // return stale data, so we should rather return null and let the query run with a full table scan.
        // Also make sure there are no long migrations in flight which may have started after starting the query
        // but not completed yet.
        if (validateMigrationStamp(migrationStamp)) {
            return entries;
        }
        return null;
    }

    protected Result runUsingPartitionScanSafely(Query query, Predicate predicate,
                                                 PartitionIdSet partitions, int migrationStamp) {

        if (!validateMigrationStamp(migrationStamp)) {
            return null;
        }

        Result result = createResult(query, partitions);
        partitionScanExecutor.execute(query.getMapName(), predicate, partitions, result);

        // If a migration is in progress or migration ownership changes, this means migrations were executed and we may
        // return stale data, so we should rather return null.
        // Also make sure there are no long migrations in progress that may have started after starting the query
        // but not completed yet.
        if (validateMigrationStamp(migrationStamp)) {
            result.completeConstruction(partitions);
            return result;
        }

        return null;
    }

    private int getMigrationStamp() {
        return mapServiceContext.getService().getMigrationStamp();
    }

    private boolean validateMigrationStamp(int migrationStamp) {
        return mapServiceContext.getService().validateMigrationStamp(migrationStamp);
    }
}
