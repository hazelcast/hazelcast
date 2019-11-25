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

import java.util.stream.IntStream;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.String.format;

/**
 * Multiple record store forced eviction.
 *
 * Evicts a percentage of entries from multiple
 * record stores which fall into same partition.
 *
 * @see SingleRecordStoreForcedEviction
 */
class MultipleRecordStoreForcedEviction implements ForcedEviction {

    @Override
    public boolean forceEvictAndRun(MapOperation mapOperation, double evictionPercentage) {
        assert evictionPercentage > 0 && evictionPercentage <= 1;

        int partitionCount = numberOfPartitions(mapOperation);
        int threadCount = threadCount(mapOperation);
        int mod = mod(mapOperation, threadCount);
        ILogger logger = mapOperation.logger();

        int evictionRetryTimes = noRetryIfEvictingAll(evictionPercentage);
        for (int i = 0; i < evictionRetryTimes; i++) {
            final int attempt = i + 1;
            try {
                IntStream.range(0, partitionCount)
                        .filter(partitionId -> partitionId % threadCount == mod)
                        .mapToObj(partitionId -> partitionMaps(mapOperation, partitionId))
                        .flatMap(maps -> maps.values().stream())
                        .filter(this::isValid)
                        .forEach(recordStore -> {

                            int sizeBeforeEviction = recordStore.size();

                            MapContainer mapContainer = recordStore.getMapContainer();
                            Evictor evictor = mapContainer.getEvictor();
                            evictor.forceEvictByPercentage(recordStore, evictionPercentage);

                            int sizeAfterEviction = recordStore.size();

                            if (logger.isFineEnabled()) {
                                String msg = "Multiple record store forced eviction "
                                        + "[attempt: %d, mainMapName: %s, evictingMapName: %s, "
                                        + "evictionPercentage: %.2f, partitionId: %d, evictedCount: %d (%d --> %d)]";

                                logger.fine(format(msg, attempt, mapOperation.getName(),
                                        recordStore.getName(), evictionPercentage,
                                        mapOperation.getPartitionId(),
                                        sizeBeforeEviction - sizeAfterEviction, sizeBeforeEviction, sizeAfterEviction));
                            }
                        });

                mapOperation.runInternal();
                return true;
            } catch (NativeOutOfMemoryError e) {
                if (evictionRetryTimes > 1) {
                    ignore(e);
                } else {
                    throw e;
                }
            }
        }

        return false;
    }
}
