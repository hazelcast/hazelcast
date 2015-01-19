/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapContainer;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Contains common fuctionallity which is required by a {@link WriteBehindProcessor}
 *
 * @param <T> the type of object to be placed in the {@link WriteBehindQueue}
 */
abstract class AbstractWriteBehindProcessor<T> implements WriteBehindProcessor<T> {

    protected final int writeBatchSize;

    protected final boolean writeCoalescing;

    protected final ILogger logger;

    protected final MapStore mapStore;

    private final SerializationService serializationService;

    public AbstractWriteBehindProcessor(MapContainer mapContainer) {
        this.serializationService = mapContainer.getMapServiceContext().getNodeEngine().getSerializationService();
        this.mapStore = mapContainer.getStore();
        this.logger = mapContainer.getMapServiceContext().getNodeEngine().getLogger(DefaultWriteBehindProcessor.class);
        MapStoreConfig mapStoreConfig = mapContainer.getMapConfig().getMapStoreConfig();
        this.writeBatchSize = mapStoreConfig.getWriteBatchSize();
        this.writeCoalescing = mapStoreConfig.isWriteCoalescing();
    }

    protected Object toObject(Object obj) {
        return serializationService.toObject(obj);
    }

    protected Data toData(Object obj) {
        return serializationService.toData(obj);
    }


    protected void sleepSeconds(long secs) {
        try {
            TimeUnit.SECONDS.sleep(secs);
        } catch (InterruptedException e) {
            logger.warning(e);
        }
    }

    /**
     * Used to group store operations.
     */
    protected enum StoreOperationType {

        DELETE {
            @Override
            boolean processSingle(Object key, Object value, MapStore mapStore) {
                mapStore.delete(key);
                return true;
            }

            @Override
            boolean processBatch(Map map, MapStore mapStore) {
                mapStore.deleteAll(map.keySet());
                return true;
            }
        },

        WRITE {
            @Override
            boolean processSingle(Object key, Object value, MapStore mapStore) {
                mapStore.store(key, value);
                return true;
            }

            @Override
            boolean processBatch(Map map, MapStore mapStore) {
                mapStore.storeAll(map);
                return true;
            }
        };

        abstract boolean processSingle(Object key, Object value, MapStore mapStore);

        abstract boolean processBatch(Map map, MapStore mapStore);
    }
}
