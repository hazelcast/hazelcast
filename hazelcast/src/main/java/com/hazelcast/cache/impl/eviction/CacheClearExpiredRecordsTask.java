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

package com.hazelcast.cache.impl.eviction;

import com.hazelcast.cache.impl.AbstractCacheRecordStore;
import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.operation.CacheClearExpiredOperation;
import com.hazelcast.internal.eviction.ClearExpiredRecordsTask;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static java.util.Collections.sort;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Clears expired entries.
 * This task provides per partition expiration operation logic.
 * Fires cleanup operations at most partition operation thread count or some factor of it in one round.
 * <ul>
 * <li>
 * {@value PROP_CLEANUP_PERCENTAGE}: Scannable percentage
 * of entries in a maps' partition in each round.
 * Default percentage is {@value DEFAULT_CLEANUP_PERCENTAGE}%.
 * </li>
 * <li>
 * {@value PROP_CLEANUP_OPERATION_COUNT}: Number of
 * scannable partitions in each round. No default value exists. Dynamically calculated against partition-count or
 * partition-thread-count.
 * </li>
 * </ul>
 *
 * <p>
 * These parameters can be set node-wide or system-wide
 * <p>
 * Node-wide setting example:
 * <pre>
 *           Config config = new Config();
 *           config.setProperty(
 *           {@value PROP_CLEANUP_OPERATION_COUNT}, "3");
 *           Hazelcast.newHazelcastInstance(config);
 *       </pre>
 * </p>
 * <p>
 * System-wide setting example:
 * <pre>
 *        System.setProperty(
 *        {@value PROP_CLEANUP_OPERATION_COUNT}, "3");
 *    </pre>
 * </p>
 */
public class CacheClearExpiredRecordsTask extends ClearExpiredRecordsTask<CachePartitionSegment>
        implements OperationResponseHandler {

    public static final String PROP_CLEANUP_PERCENTAGE = "hazelcast.internal.cache.expiration.cleanup.percentage";
    public static final String PROP_CLEANUP_OPERATION_COUNT = "hazelcast.internal.cache.expiration.cleanup.operation.count";
    public static final String PROP_TASK_PERIOD_SECONDS = "hazelcast.internal.cache.expiration.task.period.seconds";

    public static final int DEFAULT_TASK_PERIOD_SECONDS = 5;
    public static final HazelcastProperty TASK_PERIOD_SECONDS
            = new HazelcastProperty(PROP_TASK_PERIOD_SECONDS, DEFAULT_TASK_PERIOD_SECONDS, SECONDS);
    public static final int DEFAULT_CLEANUP_PERCENTAGE = 10;
    public static final int MAX_EXPIRED_KEY_COUNT_IN_BATCH = 100;

    public static final HazelcastProperty CLEANUP_PERCENTAGE
            = new HazelcastProperty(PROP_CLEANUP_PERCENTAGE, DEFAULT_CLEANUP_PERCENTAGE);
    public static final HazelcastProperty CLEANUP_OPERATION_COUNT
            = new HazelcastProperty(PROP_CLEANUP_OPERATION_COUNT);


    private final Comparator<CachePartitionSegment> partitionSegmentComparator = new Comparator<CachePartitionSegment>() {
        @Override
        public int compare(CachePartitionSegment o1, CachePartitionSegment o2) {
            final long s1 = o1.getLastCleanupTimeBeforeSorting();
            final long s2 = o2.getLastCleanupTimeBeforeSorting();
            return (s1 < s2) ? -1 : ((s1 == s2) ? 0 : 1);
        }
    };

    public CacheClearExpiredRecordsTask(NodeEngine nodeEngine, CachePartitionSegment[] containers) {
        super(nodeEngine, containers, CLEANUP_OPERATION_COUNT, CLEANUP_PERCENTAGE, TASK_PERIOD_SECONDS);
    }

    @Override
    protected boolean hasExpiredKeyToSendBackup(CachePartitionSegment container) {
        Iterator<ICacheRecordStore> iterator = container.recordStoreIterator();
        while (iterator.hasNext()) {
            ICacheRecordStore store = iterator.next();
            if (store.getExpiredKeys().size() > 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean hasRunningCleanup(CachePartitionSegment container) {
        return container.hasRunningCleanupOperation();
    }

    @Override
    protected void setHasRunningCleanup(CachePartitionSegment container, boolean status) {
        container.setRunningCleanupOperation(status);
    }

    @Override
    protected boolean isContainerEmpty(CachePartitionSegment container) {
        Iterator<ICacheRecordStore> iterator = container.recordStoreIterator();
        while (iterator.hasNext()) {
            ICacheRecordStore store = iterator.next();
            if (store.size() > 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected boolean notHaveAnyExpirableRecord(CachePartitionSegment container) {
        Iterator<ICacheRecordStore> iterator = container.recordStoreIterator();
        while (iterator.hasNext()) {
            ICacheRecordStore store = iterator.next();
            if (store.isExpirable()) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected long getLastCleanupTime(CachePartitionSegment container) {
        return container.getLastCleanupTime();
    }

    @Override
    protected void clearLeftoverExpiredKeyQueues(CachePartitionSegment container) {
        Iterator<ICacheRecordStore> iterator = container.recordStoreIterator();
        while (iterator.hasNext()) {
            ICacheRecordStore store = iterator.next();
            InvalidationQueue expiredKeys = store.getExpiredKeys();
            expiredKeys.clear();
        }
    }

    @Override
    protected void sortPartitionContainers(List<CachePartitionSegment> containers) {
        for (CachePartitionSegment segment: containers) {
            segment.storeLastCleanupTime();
        }
        sort(containers, partitionSegmentComparator);
    }

    @Override
    public void sendResponse(Operation op, Object response) {
        CachePartitionSegment container = containers[op.getPartitionId()];
        doBackupExpiration(container);
    }

    private void doBackupExpiration(CachePartitionSegment container) {
        Iterator<ICacheRecordStore> iterator = container.recordStoreIterator();
        while (iterator.hasNext()) {
            AbstractCacheRecordStore store = (AbstractCacheRecordStore) iterator.next();
            store.sendBackupExpirations(false);
        }
    }

    protected Operation createExpirationOperation(int expirationPercentage, CachePartitionSegment container) {
        int partitionId = container.getPartitionId();
        return new CacheClearExpiredOperation(expirationPercentage)
                .setNodeEngine(nodeEngine)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setPartitionId(partitionId)
                .setValidateTarget(false)
                .setServiceName(SERVICE_NAME)
                .setOperationResponseHandler(this);
    }
}
