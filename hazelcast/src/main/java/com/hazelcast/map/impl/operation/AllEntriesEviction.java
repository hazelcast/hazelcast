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
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * An {@link Eviction} operation that attempts to evict all entries from a
 * {@link com.hazelcast.map.impl.recordstore.RecordStore}
 */
class AllEntriesEviction implements Eviction {
    private final ILogger logger;
    private final MapOperation mapOperation;
    private boolean successful;

    AllEntriesEviction(ILogger logger, MapOperation mapOperation) {
        this.logger = logger;
        this.mapOperation = mapOperation;
    }

    @Override
    public void execute() {
        RecordStore recordStore = mapOperation.recordStore;
        if (recordStore == null) {
            return;
        }

        boolean isBackup = mapOperation instanceof BackupOperation;

        if (logger.isInfoEnabled()) {
            logger.info("Evicting all entries in current"
                            + " RecordStores because forced eviction was not enough!");
        }
        try {
            recordStore.evictAll(isBackup);
            recordStore.disposeDeferredBlocks();
            mapOperation.runInternal();
            successful = true;
        } catch (NativeOutOfMemoryError e) {
            ignore(e);
        }
    }

    @Override
    public boolean isSuccessful() {
        return successful;
    }
}
