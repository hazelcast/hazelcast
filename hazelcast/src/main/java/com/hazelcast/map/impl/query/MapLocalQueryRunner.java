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

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.IterationType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.map.impl.query.MapQueryEngineUtils.newQueryResult;
import static com.hazelcast.map.impl.query.MapQueryEngineUtils.newQueryResultForSinglePartition;
import static com.hazelcast.query.PagingPredicateAccessor.getNearestAnchorEntry;
import static com.hazelcast.util.SortingUtil.compareAnchor;
import static com.hazelcast.util.SortingUtil.getSortedSubList;

/**
 * Runs query operations in the calling thread (thus blocking it)
 * Query evaluation per partition is run in a sequential fashion.
 *
 * Used by {@link QueryOperation} and {@link QueryPartitionOperation} only.
 * Should not be used by proxies or any other query related objects.
 */
public class MapLocalQueryRunner {

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

    public MapLocalQueryRunner(MapServiceContext mapServiceContext, QueryOptimizer optimizer) {
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
    }

    // full query = index query (if possible), then partition-scan query
    public QueryResult run(String mapName, Predicate predicate, IterationType iterationType)
            throws ExecutionException, InterruptedException {

        int initialPartitionStateVersion = partitionService.getPartitionStateVersion();
        Collection<Integer> initialPartitions = mapServiceContext.getOwnedPartitions();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);

        // first we optimize the query
        predicate = queryOptimizer.optimize(predicate, mapContainer.getIndexes());

        // then we try to run using an index, but if that doesn't work, we'll try a full table scan
        // (this would be the point where a query-plan should be added
        // to determine if a full table scan or an index should be used)
        Collection<QueryableEntry> entries = runUsingIndexSafely(predicate, mapContainer, initialPartitionStateVersion);
        if (entries == null) {
            entries = runUsingPartitionScanSafely(mapName, predicate, initialPartitions, initialPartitionStateVersion);
        }

        QueryResult result = newQueryResult(initialPartitions.size(), iterationType, queryResultSizeLimiter);

        if (hasPartitionVersion(initialPartitionStateVersion, predicate)) {
            // if results have been returned and partition state version has not changed, set the partition IDs
            // so that caller is aware of partitions from which results were obtained
            result.addAll(entries);
            result.setPartitionIds(initialPartitions);
        }
        // else: if fallback to full table scan also failed to return any results due to migrations,
        // then return empty result set without any partition IDs set (so that it is ignored by callers)

        updateStatistics(mapContainer);

        return result;
    }

    protected Collection<QueryableEntry> runUsingIndexSafely(Predicate predicate, MapContainer mapContainer,
                                                             int initialPartitionStateVersion) {
        // if a migration is in progress, do not attempt to use an index as they may have not been created yet
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

        entries = runUsingPartitionScan(name, predicate, partitions);

        // if partition state version has changed in the meanwhile, this means migrations were executed and we may
        // return stale data, so we should rather return null (also make sure there are no long migrations in flight
        // which may have started after starting the query but not completed yet)
        if (isResultSafe(initialPartitionStateVersion)) {
            return entries;
        } else {
            return null;
        }
    }

    protected Collection<QueryableEntry> runUsingPartitionScan(
            String mapName, Predicate predicate, Collection<Integer> partitions) {
        RetryableHazelcastException storedException = null;
        Collection<QueryableEntry> result = new ArrayList<QueryableEntry>();
        for (Integer partitionId : partitions) {
            try {
                result.addAll(runUsingPartitionScanOnSinglePartition(mapName, predicate, partitionId));
            } catch (RetryableHazelcastException e) {
                // RetryableHazelcastException are stored and re-thrown later, to ensure all partitions
                // are touched as when the parallel execution was used (see discussion at
                // https://github.com/hazelcast/hazelcast/pull/5049#discussion_r28773099 for details)
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

    public QueryResult runUsingPartitionScanOnSinglePartition(
            String mapName, Predicate predicate, int partitionId, IterationType iterationType) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        predicate = queryOptimizer.optimize(predicate, mapContainer.getIndexes());
        Collection<QueryableEntry> entries = runUsingPartitionScanOnSinglePartition(mapName, predicate, partitionId);
        return newQueryResultForSinglePartition(entries, partitionId, iterationType, queryResultSizeLimiter);
    }

    @SuppressWarnings("unchecked")
    protected Collection<QueryableEntry> runUsingPartitionScanOnSinglePartition(String mapName, Predicate predicate,
                                                                                int partitionId) {
        PagingPredicate pagingPredicate = predicate instanceof PagingPredicate ? (PagingPredicate) predicate : null;
        List<QueryableEntry> resultList = new LinkedList<QueryableEntry>();

        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        Iterator<Record> iterator = partitionContainer.getRecordStore(mapName).loadAwareIterator(getNow(), false);
        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry(pagingPredicate);
        boolean useCachedValues = isUseCachedDeserializedValuesEnabled(mapContainer);
        Extractors extractors = mapServiceContext.getExtractors(mapName);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = (Data) toData(record.getKey());
            Object value = toData(
                    useCachedValues ? Records.getValueOrCachedValue(record, serializationService) : record.getValue());
            if (value == null) {
                continue;
            }
            // we want to always use CachedQueryEntry as these are short-living objects anyway
            QueryableEntry queryEntry = new CachedQueryEntry(serializationService, key, value, extractors);

            if (predicate.apply(queryEntry) && compareAnchor(pagingPredicate, queryEntry, nearestAnchorEntry)) {
                resultList.add(queryEntry);
            }
        }
        return getSortedSubList(resultList, pagingPredicate, nearestAnchorEntry);
    }

    protected <T> Object toData(T input) {
        return input;
    }

    protected boolean isUseCachedDeserializedValuesEnabled(MapContainer mapContainer) {
        CacheDeserializedValues cacheDeserializedValues = mapContainer.getMapConfig().getCacheDeserializedValues();
        switch (cacheDeserializedValues) {
            case NEVER:
                return false;
            case ALWAYS:
                return true;
            default:
                // if index exists then cached value is already set -> let's use it
                return mapContainer.getIndexes().hasIndex();
        }
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    /**
     * Check whether migrations of owner partition are currently executed.
     *
     * If a migration is in progress, do not attempt to use an index as they may have not been created yet.
     * {@link com.hazelcast.map.impl.MapService#getOwnerMigrationsInFlight} returns the number of currently
     * executing migrations (for which beforeMigration has been executed but commit/rollback is not yet executed).
     *
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
     * @param initialPartitionStateVersion the initial version of the partition state
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
