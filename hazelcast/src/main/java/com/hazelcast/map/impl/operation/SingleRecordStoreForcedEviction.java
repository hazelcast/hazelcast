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

package com.hazelcast.map.impl.operation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.String.format;

/**
 * Single record store forced eviction.
 *
 * Evicts a percentage of entries from a single record store.
 *
 * @see MultipleRecordStoreForcedEviction
 */
class SingleRecordStoreForcedEviction implements ForcedEviction {

    @Override
    public boolean forceEvictAndRun(MapOperation mapOperation, double evictionPercentage) {
        assert evictionPercentage > 0 && evictionPercentage <= 1;

        RecordStore recordStore = mapOperation.recordStore;
        if (!ForcedEviction.isValid(recordStore)) {
            return false;
        }

        ILogger logger = mapOperation.logger();

        int retryCount = retryCount(evictionPercentage);
        for (int i = 0; i < retryCount; i++) {
            if (logger.isFineEnabled()) {
                if (logger.isFineEnabled()) {
                    String msg = "Single record store forced eviction [attemptNumber: %d, mapName: %s, "
                            + "evictionPercentage:%.2f, partitionId: %d]";
                    logger.fine(format(msg, (i + 1), mapOperation.getName(),
                            evictionPercentage, mapOperation.getPartitionId()));
                }
            }

            try {
                Evictor evictor = recordStore.getMapContainer().getEvictor();
                evictor.forceEvictByPercentage(recordStore, evictionPercentage);
                mapOperation.runInternal();
                return true;
            } catch (NativeOutOfMemoryError e) {
                ignore(e);
            }
        }
        return false;
    }
}
