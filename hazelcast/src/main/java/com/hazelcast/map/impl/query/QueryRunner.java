/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

/**
 * Runs query operations in the calling thread (thus blocking it)
 * <p>
 * Used by query operations only: QueryOperation & QueryPartitionOperation
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

    public QueryRunner(MapServiceContext mapServiceContext, QueryOptimizer optimizer,
                       PartitionScanExecutor partitionScanExecutor, ResultProcessorRegistry resultProcessorRegistry) {
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
    }

    // full query = index query (if possible), then partition-scan query
    public Result runIndexOrPartitionScanQueryOnOwnedPartitions(Query query)
            throws ExecutionException, InterruptedException {

        int migrationStamp = getMigrationStamp();
        Collection<Integer> initialPartitions = mapServiceContext.getOwnedPartitions();

        MapContainer mapContainer = mapServiceContext.getMapContainer(query.getMapName());

        // first we optimize the query
        Predicate predicate = queryOptimizer.optimize(query.getPredicate(), mapContainer.getIndexes());

        // then we try to run using an index, but if that doesn't work, we'll try a full table scan
        // This would be the point where a query-plan should be added. It should determine f a full table scan
        // or an index should be used.
        Collection<QueryableEntry> entries = runUsingIndexSafely(predicate, mapContainer, migrationStamp);
        if (entries == null) {
            entries = runUsingPartitionScanSafely(query.getMapName(), predicate, initialPartitions, migrationStamp);
        }

        updateStatistics(mapContainer);
        if (entries != null) {
            // if results have been returned and partition state has not changed, set the partition IDs
            // so that caller is aware of partitions from which results were obtained.
            return populateTheResult(query, entries, initialPartitions);
        } else {
            // else: if fallback to full table scan also failed to return any results due to migrations,
            // then return empty result set without any partition IDs set (so that it is ignored by callers).
            return resultProcessorRegistry.get(query.getResultType()).populateResult(query,
                    queryResultSizeLimiter.getNodeResultLimit(initialPartitions.size()));
        }
    }

    private Result populateTheResult(Query query, Collection<QueryableEntry> entries, Collection<Integer> initialPartitions) {
        ResultProcessor processor = resultProcessorRegistry.get(query.getResultType());
        return processor.populateResult(query, queryResultSizeLimiter
                .getNodeResultLimit(initialPartitions.size()), entries, initialPartitions
        );
    }

    private Collection<QueryableEntry> runUsingIndexSafely(Predicate predicate, MapContainer mapContainer,
                                                           int migrationStamp) {

        // If a migration is in progress or migration ownership changes,
        // do not attempt to use an index as they may have not been created yet.
        if (!validateMigrationStamp(migrationStamp)) {
            return null;
        }

        Collection<QueryableEntry> entries = mapContainer.getIndexes().query(predicate);
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

    protected Collection<QueryableEntry> runUsingPartitionScanSafely(String name, Predicate predicate,
                                                                     Collection<Integer> partitions, int migrationStamp)
            throws InterruptedException, ExecutionException {

        Collection<QueryableEntry> entries;
        if (!validateMigrationStamp(migrationStamp)) {
            return null;
        }

        entries = partitionScanExecutor.execute(name, predicate, partitions);

        // If a migration is in progress or migration ownership changes, this means migrations were executed and we may
        // return stale data, so we should rather return null.
        // Also make sure there are no long migrations in flight which may have started after starting the query
        // but not completed yet.
        if (validateMigrationStamp(migrationStamp)) {
            return entries;
        }
        return null;
    }

    Result runPartitionScanQueryOnGivenOwnedPartition(Query query, int partitionId)
            throws ExecutionException, InterruptedException {
        MapContainer mapContainer = mapServiceContext.getMapContainer(query.getMapName());
        Predicate predicate = queryOptimizer.optimize(query.getPredicate(), mapContainer.getIndexes());
        Collection<QueryableEntry> entries = partitionScanExecutor.execute(query.getMapName(), predicate,
                Collections.singletonList(partitionId));
        return populateTheResult(query, entries, Collections.singletonList(partitionId));
    }

    private int getMigrationStamp() {
        return mapServiceContext.getService().getMigrationStamp();
    }

    private boolean validateMigrationStamp(int migrationStamp) {
        return mapServiceContext.getService().validateMigrationStamp(migrationStamp);
    }

    private void updateStatistics(MapContainer mapContainer) {
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            LocalMapStatsImpl localStats = localMapStatsProvider.getLocalMapStatsImpl(mapContainer.getName());
            localStats.incrementOtherOperations();
        }
    }
}
