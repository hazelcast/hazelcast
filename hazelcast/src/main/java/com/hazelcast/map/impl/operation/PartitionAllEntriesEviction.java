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

package com.hazelcast.map.impl.operation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.util.stream.IntStream;

/**
 * An {@link Eviction} operation that attempts to evict all an {@link com.hazelcast.map.IMap} partition entries
 * of current thread
 */
class PartitionAllEntriesEviction extends PartitionEviction {
    private final ThreadLocal<Boolean> successful = ThreadLocal.withInitial(() -> false);

    @Override
    public void execute(int retries, MapOperation mapOperation, ILogger logger) {
        successful.set(false);
        if (logger.isInfoEnabled()) {
            logger.info("Evicting all entries in other RecordStores owned by the same partition thread"
                            + " because forced eviction was not enough!");
        }

        boolean isBackup = backupOperation(mapOperation);
        int partitionCount = numberOfPartitions(mapOperation);
        int threadCount = threadCount(mapOperation);
        int mod = mod(mapOperation, threadCount);

        IntStream.range(0, partitionCount)
            .filter(partitionId -> partitionId % threadCount == mod)
            .mapToObj(partitionId -> partitionMaps(mapOperation, partitionId))
            .flatMap(map -> map.values().stream())
            .filter(this::nativeFormatWithEvictionPolicy)
            .forEach(recordStore -> {
                recordStore.evictAll(isBackup);
                recordStore.disposeDeferredBlocks();
            });
        mapOperation.runInternal();
        successful.set(true);
    }

    private boolean backupOperation(MapOperation mapOperation) {
        return mapOperation instanceof BackupOperation;
    }

    @Override
    public boolean isSuccessful() {
        return successful.get();
    }
}
