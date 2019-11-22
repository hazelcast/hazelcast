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
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.memory.NativeOutOfMemoryError;

import java.util.Objects;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.String.format;

/**
 * An {@link ForcedEviction} operation that attempts to evict entries from current thread partition
 * of an {@link com.hazelcast.map.impl.recordstore.RecordStore}
 */
class PartitionRecordStoreForcedEviction extends PartitionForcedEviction {
    @Override
    public boolean execute(int retries, MapOperation mapOperation, ILogger logger) {
        int partitionCount = numberOfPartitions(mapOperation);
        int threadCount = threadCount(mapOperation);
        int mod = mod(mapOperation, threadCount);

        for (int i = 0; i < retries; i++) {
            try {
                if (logger.isFineEnabled()) {
                    logger.fine(format("Applying forced eviction on other RecordStores owned by the same partition thread"
                                           + " (map %s, partitionId: %d", mapOperation.getName(), mapOperation.getPartitionId()));
                }

                IntStream.range(0, partitionCount)
                    .filter(partitionId -> partitionId % threadCount == mod)
                    .mapToObj(partitionId -> partitionMaps(mapOperation, partitionId))
                    .flatMap(maps -> maps.values().stream())
                    .filter(Objects::nonNull)
                    .filter(this::nativeFormatWithEvictionPolicy)
                    .forEach(recordStore -> {
                        MapContainer mapContainer = recordStore.getMapContainer();
                        Evictor evictor = mapContainer.getEvictor();
                        evictor.forceEvict(recordStore);
                    });
                mapOperation.runInternal();
                return true;
            } catch (NativeOutOfMemoryError e) {
                ignore(e);
            }
        }
        return false;
    }
}
