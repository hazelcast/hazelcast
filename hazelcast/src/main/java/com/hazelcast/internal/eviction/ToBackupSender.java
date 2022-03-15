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

package com.hazelcast.internal.eviction;

import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationQueue;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.function.BiFunction;

/**
 * Helper class to create and send backup expiration operations.
 *
 * @param <RS> type of record store
 */
public final class ToBackupSender<RS> {

    private static final int TARGET_BATCH_SIZE = 100;

    private final String serviceName;
    private final OperationService operationService;
    private final BiFunction<Integer, Integer, Boolean> backupOpFilter;
    private final BiFunction<RS, Collection<ExpiredKey>, Operation> backupOpSupplier;

    private ToBackupSender(String serviceName,
                           BiFunction<RS, Collection<ExpiredKey>, Operation> backupOpSupplier,
                           BiFunction<Integer, Integer, Boolean> backupOpFilter,
                           NodeEngine nodeEngine) {
        this.serviceName = serviceName;
        this.backupOpFilter = backupOpFilter;
        this.backupOpSupplier = backupOpSupplier;
        this.operationService = nodeEngine.getOperationService();
    }

    static <S> ToBackupSender<S> newToBackupSender(String serviceName,
                                                   BiFunction<S, Collection<ExpiredKey>, Operation> operationSupplier,
                                                   BiFunction<Integer, Integer, Boolean> backupOpFilter,
                                                   NodeEngine nodeEngine) {
        return new ToBackupSender<S>(serviceName, operationSupplier, backupOpFilter, nodeEngine);
    }

    private static Collection<ExpiredKey> pollExpiredKeys(Queue<ExpiredKey> expiredKeys) {
        Collection<ExpiredKey> polledKeys = new LinkedList<>();

        do {
            ExpiredKey expiredKey = expiredKeys.poll();
            if (expiredKey == null) {
                break;
            }
            polledKeys.add(expiredKey);
        } while (true);

        return polledKeys;
    }

    public void trySendExpiryOp(RS recordStore, InvalidationQueue expiredKeyQueue,
                                int backupReplicaCount, int partitionId, boolean sendIfAtBatchSize) {
        if (sendIfAtBatchSize && expiredKeyQueue.size() < TARGET_BATCH_SIZE) {
            return;
        }
        Collection<ExpiredKey> expiredKeys = pollExpiredKeys(expiredKeyQueue);
        if (CollectionUtil.isEmpty(expiredKeys)) {
            return;
        }
        // send expired keys to all backups
        invokeBackupExpiryOperation(expiredKeys, backupReplicaCount, partitionId, recordStore);
    }

    public void invokeBackupExpiryOperation(Collection<ExpiredKey> expiredKeys, int backupReplicaCount,
                                            int partitionId, RS recordStore) {
        for (int replicaIndex = 1; replicaIndex < backupReplicaCount + 1; replicaIndex++) {
            if (backupOpFilter.apply(partitionId, replicaIndex)) {
                Operation operation = backupOpSupplier.apply(recordStore, expiredKeys);
                operationService.invokeOnPartitionAsync(serviceName, operation, partitionId, replicaIndex);
            }
        }
    }
}
