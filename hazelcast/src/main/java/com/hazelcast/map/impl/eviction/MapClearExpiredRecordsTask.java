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

package com.hazelcast.map.impl.eviction;

import com.hazelcast.internal.eviction.ClearExpiredRecordsTask;
import com.hazelcast.internal.eviction.ExpiredKey;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.operation.EvictBatchBackupOperation;
import com.hazelcast.map.impl.operation.MapClearExpiredOperation;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static java.util.Collections.sort;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Clears expired entries (TTL & idle).
 * <p>
 * This task provides per partition expiration operation logic. (not
 * per map, not per record store). Fires cleanup operations at most
 * partition operation thread count or some factor of it in one round.
 * <ul>
 * <li>
 * {@value PROP_TASK_PERIOD_SECONDS}: The time between termination of
 * one execution of this task and the commencement of the next one.
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
 * <li>
 * {@value PROP_PRIMARY_DRIVES_BACKUP}: Used to enable/disable
 * management of backup expiration from primary. This
 * can only be used with max idle seconds expiration.
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
 *       </pre>
 * <p>
 * System-wide setting example:
 * <pre>
 *        System.setProperty({@value PROP_CLEANUP_OPERATION_COUNT}, "3");
 *    </pre>
 */
public class MapClearExpiredRecordsTask
        extends ClearExpiredRecordsTask<PartitionContainer, RecordStore> {

    public static final String PROP_CLEANUP_ENABLED = "hazelcast.internal.map.expiration.cleanup.enabled";
    public static final String PROP_PRIMARY_DRIVES_BACKUP = "hazelcast.internal.map.expiration.primary.drives_backup";
    public static final String PROP_CLEANUP_PERCENTAGE = "hazelcast.internal.map.expiration.cleanup.percentage";
    public static final String PROP_CLEANUP_OPERATION_COUNT = "hazelcast.internal.map.expiration.cleanup.operation.count";
    public static final String PROP_TASK_PERIOD_SECONDS = "hazelcast.internal.map.expiration.task.period.seconds";

    private static final boolean DEFAULT_PRIMARY_DRIVES_BACKUP = true;
    private static final int DEFAULT_TASK_PERIOD_SECONDS = 5;
    private static final int DEFAULT_CLEANUP_PERCENTAGE = 10;
    private static final HazelcastProperty PRIMARY_DRIVES_BACKUP
            = new HazelcastProperty(PROP_PRIMARY_DRIVES_BACKUP, DEFAULT_PRIMARY_DRIVES_BACKUP);
    private static final HazelcastProperty TASK_PERIOD_SECONDS
            = new HazelcastProperty(PROP_TASK_PERIOD_SECONDS, DEFAULT_TASK_PERIOD_SECONDS, SECONDS);
    private static final HazelcastProperty CLEANUP_PERCENTAGE
            = new HazelcastProperty(PROP_CLEANUP_PERCENTAGE, DEFAULT_CLEANUP_PERCENTAGE);
    private static final HazelcastProperty CLEANUP_OPERATION_COUNT
            = new HazelcastProperty(PROP_CLEANUP_OPERATION_COUNT);
    private static final HazelcastProperty CLEANUP_ENABLED
            = new HazelcastProperty(PROP_CLEANUP_ENABLED, true);

    private final boolean primaryDrivesEviction;

    private final Comparator<PartitionContainer> partitionContainerComparator = (o1, o2) -> {
        final long s1 = o1.getLastCleanupTimeCopy();
        final long s2 = o2.getLastCleanupTimeCopy();
        return Long.compare(s1, s2);
    };

    public MapClearExpiredRecordsTask(PartitionContainer[] containers, NodeEngine nodeEngine) {
        super(SERVICE_NAME, containers, CLEANUP_ENABLED, CLEANUP_OPERATION_COUNT,
                CLEANUP_PERCENTAGE, TASK_PERIOD_SECONDS, nodeEngine);
        this.primaryDrivesEviction = nodeEngine.getProperties().getBoolean(PRIMARY_DRIVES_BACKUP);
    }

    public boolean canPrimaryDriveExpiration() {
        return primaryDrivesEviction;
    }

    @Override
    public void tryToSendBackupExpiryOp(RecordStore store, boolean sendIfAtBatchSize) {
        if (!canPrimaryDriveExpiration()) {
            return;
        }

        InvalidationQueue expiredKeys = store.getExpiredKeysQueue();
        int totalBackupCount = store.getMapContainer().getTotalBackupCount();
        int partitionId = store.getPartitionId();

        toBackupSender.trySendExpiryOp(store, expiredKeys,
                totalBackupCount, partitionId, sendIfAtBatchSize);
    }

    @Override
    public Iterator<RecordStore> storeIterator(PartitionContainer container) {
        return container.getMaps().values().iterator();
    }

    @Override
    protected Operation newPrimaryExpiryOp(int expirationPercentage, PartitionContainer container) {
        int partitionId = container.getPartitionId();
        return new MapClearExpiredOperation(expirationPercentage)
                .setNodeEngine(nodeEngine)
                .setCallerUuid(nodeEngine.getLocalMember().getUuid())
                .setPartitionId(partitionId)
                .setValidateTarget(false)
                .setServiceName(SERVICE_NAME);
    }

    @Override
    protected Operation newBackupExpiryOp(RecordStore store, Collection<ExpiredKey> expiredKeys) {
        return new EvictBatchBackupOperation(store.getName(), expiredKeys, store.size());
    }

    @Override
    protected void sortPartitionContainers(List<PartitionContainer> partitionContainers) {
        // Set last clean-up time before sorting.
        for (PartitionContainer partitionContainer : partitionContainers) {
            partitionContainer.setLastCleanupTimeCopy(partitionContainer.getLastCleanupTime());
        }

        sort(partitionContainers, partitionContainerComparator);
    }

    @Override
    protected ProcessablePartitionType getProcessablePartitionType() {
        return ProcessablePartitionType.PRIMARY_OR_BACKUP_PARTITION;
    }

    protected void equalizeBackupSizeWithPrimary(PartitionContainer container) {
        if (!canPrimaryDriveExpiration()) {
            return;
        }

        ConcurrentMap<String, RecordStore> maps = container.getMaps();
        for (RecordStore recordStore : maps.values()) {
            int totalBackupCount = recordStore.getMapContainer().getTotalBackupCount();
            toBackupSender.invokeBackupExpiryOperation(Collections.emptyList(),
                    totalBackupCount, recordStore.getPartitionId(), recordStore);
        }
    }

    @Override
    protected boolean hasExpiredKeyToSendBackup(PartitionContainer container) {
        long size = 0L;
        ConcurrentMap<String, RecordStore> maps = container.getMaps();
        for (RecordStore store : maps.values()) {
            size += store.getExpiredKeysQueue().size();
            if (size > 0L) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean hasRunningCleanup(PartitionContainer container) {
        return container.hasRunningCleanup();
    }

    @Override
    protected void setHasRunningCleanup(PartitionContainer container) {
        container.setHasRunningCleanup(true);
    }

    /**
     * Here we check if that partition has any expirable record or not,
     * if no expirable record exists in that partition no need to fire
     * an expiration operation.
     *
     * @param partitionContainer corresponding partition container.
     * @return <code>true</code> if no expirable record in that
     * partition <code>false</code> otherwise.
     */
    @Override
    protected boolean notHaveAnyExpirableRecord(PartitionContainer partitionContainer) {
        boolean notExist = true;
        final ConcurrentMap<String, RecordStore> maps = partitionContainer.getMaps();
        for (RecordStore store : maps.values()) {
            if (store.isExpirable()) {
                notExist = false;
                break;
            }
        }
        return notExist;
    }

    @Override
    protected boolean isContainerEmpty(PartitionContainer container) {
        long size = 0L;
        ConcurrentMap<String, RecordStore> maps = container.getMaps();
        for (RecordStore store : maps.values()) {
            size += store.size();
            if (size > 0L) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected long getLastCleanupTime(PartitionContainer container) {
        return container.getLastCleanupTime();
    }

    @Override
    public String toString() {
        return MapClearExpiredRecordsTask.class.getName();
    }
}
