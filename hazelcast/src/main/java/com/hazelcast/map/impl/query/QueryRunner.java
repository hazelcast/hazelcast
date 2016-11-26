/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.partition.IPartitionService;

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
    protected final IPartitionService partitionService;
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
        this.partitionService = nodeEngine.getPartitionService();
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
    public Result run(Query query)
            throws ExecutionException, InterruptedException {

        int initialPartitionStateVersion = partitionService.getPartitionStateVersion();
        Collection<Integer> initialPartitions = mapServiceContext.getOwnedPartitions();
        MapContainer mapContainer = mapServiceContext.getMapContainer(query.getMapName());

        // first we optimize the query
        Predicate predicate = queryOptimizer.optimize(query.getPredicate(), mapContainer.getIndexes());

        // then we try to run using an index, but if that doesn't work, we'll try a full table scan
        // This would be the point where a query-plan should be added. It should determine f a full table scan
        // or an index should be used.
        Collection<QueryableEntry> entries = runUsingIndexSafely(predicate, mapContainer, initialPartitionStateVersion);
        if (entries == null) {
            entries = runUsingPartitionScanSafely(query.getMapName(), predicate, initialPartitions, initialPartitionStateVersion);
        }

        updateStatistics(mapContainer);
        if (hasPartitionVersion(initialPartitionStateVersion, predicate)) {
            // if results have been returned and partition state version has not changed, set the partition IDs
            // so that caller is aware of partitions from which results were obtained.
            return populateTheResult(query, entries, initialPartitions);
        } else {
            // else: if fallback to full table scan also failed to return any results due to migrations,
            // then return empty result set without any partition IDs set (so that it is ignored by callers).
            return resultProcessorRegistry.get(query.getResultType()).populateResult(query,
                    queryResultSizeLimiter.getNodeResultLimit(initialPartitions.size()));
        }
    }

    protected Result populateTheResult(Query query, Collection<QueryableEntry> entries, Collection<Integer> initialPartitions) {
        ResultProcessor processor = resultProcessorRegistry.get(query.getResultType());
        return processor.populateResult(query, queryResultSizeLimiter
                .getNodeResultLimit(initialPartitions.size()), entries, initialPartitions
        );
    }

    protected Collection<QueryableEntry> runUsingIndexSafely(
            Predicate predicate, MapContainer mapContainer, int initialPartitionStateVersion) {
        // if a migration is in progress, do not attempt to use an index as they may have not been created yet.
        // MapService.getMigrationsInFlight() returns the number of currently executing migrations (for which
        // beforeMigration has been executed but commit/rollback is not yet executed).
        // This is a temporary fix for 3.7, the actual issue will be addressed with an additional migration hook in 3.8.
        // see https://github.com/hazelcast/hazelcast/issues/6471 & https://github.com/hazelcast/hazelcast/issues/8046
        if (hasOwnerMigrationsInFlight()) {
            return null;
        }

        Collection<QueryableEntry> entries = runUsingIndex(predicate, mapContainer);
        if (entries == null) {
            return null;
        }

        // If partition state version has changed in the meanwhile, this means migrations were executed and we may
        // return stale data, so we should rather return null and let the query run with a full table scan.
        // Also make sure there are no long migrations in flight which may have started after starting the query
        // but not completed yet.
        if (isResultSafe(initialPartitionStateVersion)) {
            return entries;
        } else {
            return null;
        }
    }

    protected Collection<QueryableEntry> runUsingIndex(Predicate predicate, MapContainer mapContainer) {
        return mapContainer.getIndexes().query(predicate);
    }

    protected Collection<QueryableEntry> runUsingPartitionScanSafely(
            String name, Predicate predicate, Collection<Integer> partitions, int initialPartitionStateVersion)
            throws InterruptedException, ExecutionException {

        Collection<QueryableEntry> entries;
        if (hasOwnerMigrationsInFlight()) {
            return null;
        }

        entries = partitionScanExecutor.execute(name, predicate, partitions);

        // If partition state version has changed in the meanwhile, this means migrations were executed and we may
        // return stale data, so we should rather return null.
        // Also make sure there are no long migrations in flight which may have started after starting the query
        // but not completed yet.
        if (isResultSafe(initialPartitionStateVersion)) {
            return entries;
        } else {
            return null;
        }
    }

    public Result runUsingPartitionScanOnSinglePartition(
            Query query, int partitionId) throws ExecutionException, InterruptedException {
        Collection<QueryableEntry> entries = doRunUsingPartitionScanOnSinglePartition(query.getMapName(),
                query.getPredicate(), partitionId);
        return populateTheResult(query, entries, Collections.singletonList(partitionId));
    }

    protected Collection<QueryableEntry> doRunUsingPartitionScanOnSinglePartition(
            String mapName, Predicate originalPredicate, int partitionId) throws ExecutionException, InterruptedException {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        Predicate predicate = queryOptimizer.optimize(originalPredicate, mapContainer.getIndexes());
        return partitionScanExecutor.execute(mapName, predicate, Collections.singletonList(partitionId));
    }

    /**
     * Check whether migrations of owner partition are currently executed.
     * If a migration is in progress, do not attempt to use an index as they may have not been created yet.
     * MapService.getMigrationsInFlight() returns the number of currently executing migrations (for which
     * beforeMigration has been executed but commit/rollback is not yet executed).
     * This check is a temporary fix for 3.7, the actual issue will be addressed with an additional migration hook in 3.8.
     * see https://github.com/hazelcast/hazelcast/issues/6471 & https://github.com/hazelcast/hazelcast/issues/8046
     *
     * @return {@code true} if owner partition migrations are currently being executed, otherwise false.
     * @see com.hazelcast.spi.impl.CountingMigrationAwareService
     */
    protected boolean hasOwnerMigrationsInFlight() {
        return mapServiceContext.getService().getOwnerMigrationsInFlight() > 0;
    }

    /**
     * Check whether partition state version has changed since {@code initialPartitionStateVersion}.
     *
     * @param initialPartitionStateVersion the initial partition state version to compare against
     * @return {@code true} if current partition state version is not equal to {@code initialPartitionStateVersion}
     */
    protected boolean hasPartitionStateVersionChanged(int initialPartitionStateVersion) {
        return initialPartitionStateVersion != partitionService.getPartitionStateVersion();
    }

    /**
     * Check whether results obtained since partition state version was at {@code initialPartitionStateVersion} are safe
     * to be returned to the caller. Effectively this method checks:
     * <ul>
     * <li>whether owner migrations are currently in flight; if there are any, then results are considered flawed</li>
     * <li>whether current partition state version has changed, implying that some migrations were executed in the
     * meanwhile, so results are again considered flawed</li>
     * </ul>
     *
     * @param initialPartitionStateVersion
     * @return {@code true} if no owner migrations are currently executing and {@code initialPartitionStateVersion} is
     * the same as the current partition state version, otherwise {@code false}.
     */
    protected boolean isResultSafe(int initialPartitionStateVersion) {
        return !hasOwnerMigrationsInFlight() && !hasPartitionStateVersionChanged(initialPartitionStateVersion);
    }

    protected boolean hasPartitionVersion(int expectedVersion, Predicate predicate) {
        if (hasPartitionStateVersionChanged(expectedVersion)) {
            logger.info("Partition assignments changed while executing query: " + predicate);
            return false;
        }
        return true;
    }

    protected void updateStatistics(MapContainer mapContainer) {
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            LocalMapStatsImpl localStats = localMapStatsProvider.getLocalMapStatsImpl(mapContainer.getName());
            localStats.incrementOtherOperations();
        }
    }

}
