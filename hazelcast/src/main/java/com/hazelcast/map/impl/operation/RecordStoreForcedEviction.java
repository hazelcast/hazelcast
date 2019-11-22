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
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.NativeOutOfMemoryError;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.String.format;

/**
 * An {@link ForcedEviction} operation that attempts to force evict entries from a
 * {@link com.hazelcast.map.impl.recordstore.RecordStore}
 */
class RecordStoreForcedEviction implements ForcedEviction {
    @Override
    public boolean execute(int retries, MapOperation mapOperation, ILogger logger) {
        if (doesNotHaveRecordStore(mapOperation)) {
            return false;
        }

        RecordStore recordStore = mapOperation.recordStore;
        if (!nativeFormatWithEvictionPolicy(recordStore)) {
            return false;
        }

        MapContainer mapContainer = recordStore.getMapContainer();
        Evictor evictor = mapContainer.getEvictor();

        for (int i = 0; i < retries; i++) {
            if (logger.isFineEnabled()) {
                logger.fine(format(
                    "Applying forced eviction on current RecordStore (map %s, partitionId: %d)!",
                    mapOperation.getName(),
                    mapOperation.getPartitionId()
                ));
            }

            try {
                evictor.forceEvict(recordStore);
                mapOperation.runInternal();
                return true;
            } catch (NativeOutOfMemoryError e) {
                ignore(e);
            }
        }
        return false;
    }
}
