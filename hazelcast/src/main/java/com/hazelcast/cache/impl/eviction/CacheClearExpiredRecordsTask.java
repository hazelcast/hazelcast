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

package com.hazelcast.cache.impl.eviction;

import com.hazelcast.cache.impl.CachePartitionSegment;
import com.hazelcast.cache.impl.ICacheRecordStore;
import com.hazelcast.cache.impl.operation.CacheClearExpiredOperation;
import com.hazelcast.cache.impl.operation.CacheExpireBatchBackupOperation;
import com.hazelcast.internal.eviction.ClearExpiredRecordsTask;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static java.util.Collections.sort;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Clears expired entries.
 * <p>
 * This task provides per partition expiration
 * operation logic. Fires cleanup operations at most partition
 * operation thread count or some factor of it in one round.
 *
 * <ul>
 * <li>
 * {@value PROP_TASK_PERIOD_SECONDS}: Interval, in seconds,
 * at which the background expiration task is going to run.
 * Default value is {@value DEFAULT_TASK_PERIOD_SECONDS} seconds.
 * </li>
 * <li>
 * {@value PROP_CLEANUP_PERCENTAGE}: Scannable percentage
 * of entries in a maps' partition in each round. Default
 * percentage is {@value DEFAULT_CLEANUP_PERCENTAGE}%.
 * </li>
 * <li>
 * {@value PROP_CLEANUP_OPERATION_COUNT}: Number of scannable
 * partitions in each round. No default value exists. Dynamically
 * calculated against partition-count or partition-thread-count.
 * </li>
 * </ul>
 *
 * <p>
 * These parameters can be set node-wide or system-wide
 * <p>
 * Node-wide setting example:
 * <pre>
 *           Config config = new Config();
 *           config.setProperty({@value PROP_CLEANUP_OPERATION_COUNT}, "3");
 *           Hazelcast.newHazelcastInstance(config);
 * </pre>
 * <p>
 * System-wide setting example:
 * <pre>
 *        System.setProperty({@value PROP_CLEANUP_OPERATION_COUNT}, "3");
 * </pre>
 */
public class CacheClearExpiredRecordsTask
        extends ClearExpiredRecordsTask<CachePartitionSegment, ICacheRecordStore> {

    public static final String PROP_CLEANUP_ENABLED = "hazelcast.internal.cache.expiration.cleanup.enabled";
    public static final String PROP_CLEANUP_PERCENTAGE = "hazelcast.internal.cache.expiration.cleanup.percentage";
    public static final String PROP_TASK_PERIOD_SECONDS = "hazelcast.internal.cache.expiration.task.period.seconds";
    public static final String PROP_CLEANUP_OPERATION_COUNT = "hazelcast.internal.cache.expiration.cleanup.operation.count";

    private static final int DEFAULT_TASK_PERIOD_SECONDS = 5;
    private static final int DEFAULT_CLEANUP_PERCENTAGE = 10;
    private static final HazelcastProperty TASK_PERIOD_SECONDS
            = new HazelcastProperty(PROP_TASK_PERIOD_SECONDS, DEFAULT_TASK_PERIOD_SECONDS, SECONDS);
    private static final HazelcastProperty CLEANUP_PERCENTAGE
            = new HazelcastProperty(PROP_CLEANUP_PERCENTAGE, DEFAULT_CLEANUP_PERCENTAGE);
    private static final HazelcastProperty CLEANUP_OPERATION_COUNT = new HazelcastProperty(PROP_CLEANUP_OPERATION_COUNT);
    private static final HazelcastProperty CLEANUP_ENABLED = new HazelcastProperty(PROP_CLEANUP_ENABLED, true);

    private final Comparator<CachePartitionSegment> partitionSegmentComparator = (o1, o2) -> {
        long s1 = o1.getLastCleanupTimeBeforeSorting();
        long s2 = o2.getLastCleanupTimeBeforeSorting();
        return (s1 < s2) ? -1 : ((s1 == s2) ? 0 : 1);
    };

    public CacheClearExpiredRecordsTask(CachePartitionSegment[] containers, NodeEngine nodeEngine) {
        super(SERVICE_NAME, containers, CLEANUP_ENABLED, CLEANUP_OPERATION_COUNT,
                CLEANUP_PERCENTAGE, TASK_PERIOD_SECONDS, nodeEngine);
    }

    @Override
    public void tryToSendBackupExpiryOp(ICacheRecordStore store, boolean sendIfAtBatchSize) {
        InvalidationQueue<ExpiredKey> expiredKeys = store.getExpiredKeysQueue();
        int totalBackupCount = store.getConfig().getTotalBackupCount();
        int partitionId = store.getPartitionId();

        toBackupSender.trySendExpiryOp(store, expiredKeys,
                totalBackupCount, partitionId, sendIfAtBatchSize);
    }

    @Override
    public Iterator<ICacheRecordStore> storeIterator(CachePartitionSegment container) {
        return container.recordStoreIterator();
    }

    @Override
    protected Operation newPrimaryExpiryOp(int expirationPercentage, CachePartitionSegment container) {
        return new CacheClearExpiredOperation(expirationPercentage)
                .setNodeEngine(nodeEngine)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setPartitionId(container.getPartitionId())
                .setValidateTarget(false)
                .setServiceName(SERVICE_NAME);
    }

    @Override
    protected Operation newBackupExpiryOp(ICacheRecordStore store, Collection<ExpiredKey> expiredKeys) {
        return new CacheExpireBatchBackupOperation(store.getName(), expiredKeys, store.size());
    }

    @Override
    protected void equalizeBackupSizeWithPrimary(CachePartitionSegment container) {
        Iterator<ICacheRecordStore> iterator = container.recordStoreIterator();
        while (iterator.hasNext()) {
            ICacheRecordStore recordStore = iterator.next();
            int totalBackupCount = recordStore.getConfig().getTotalBackupCount();
            int partitionId = recordStore.getPartitionId();
            toBackupSender.invokeBackupExpiryOperation(Collections.<ExpiredKey>emptyList(),
                    totalBackupCount, partitionId, recordStore);
        }
    }

    @Override
    protected boolean hasExpiredKeyToSendBackup(CachePartitionSegment container) {
        Iterator<ICacheRecordStore> iterator = container.recordStoreIterator();
        while (iterator.hasNext()) {
            ICacheRecordStore store = iterator.next();
            if (store.getExpiredKeysQueue().size() > 0) {
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
    protected void setHasRunningCleanup(CachePartitionSegment container) {
        container.setRunningCleanupOperation(true);
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
    protected void sortPartitionContainers(List<CachePartitionSegment> containers) {
        for (CachePartitionSegment segment : containers) {
            segment.storeLastCleanupTime();
        }
        sort(containers, partitionSegmentComparator);
    }

    @Override
    protected ProcessablePartitionType getProcessablePartitionType() {
        return ProcessablePartitionType.PRIMARY_PARTITION;
    }

    @Override
    public String toString() {
        return CacheClearExpiredRecordsTask.class.getName();
    }
}
