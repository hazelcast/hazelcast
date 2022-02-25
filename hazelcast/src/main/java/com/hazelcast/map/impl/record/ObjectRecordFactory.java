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

package com.hazelcast.map.impl.record;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapContainer;

import javax.annotation.Nonnull;

import static com.hazelcast.map.impl.eviction.Evictor.NULL_EVICTOR;

public class ObjectRecordFactory implements RecordFactory<Object> {

    private final MapContainer mapContainer;
    private final SerializationService serializationService;

    public ObjectRecordFactory(MapContainer mapContainer,
                               SerializationService serializationService) {
        this.serializationService = serializationService;
        this.mapContainer = mapContainer;
    }

    @Override
    public Record<Object> newRecord(Data key, Object value) {
        MapConfig mapConfig = mapContainer.getMapConfig();
        boolean perEntryStatsEnabled = mapConfig.isPerEntryStatsEnabled();
        boolean hasEviction = mapContainer.getEvictor() != NULL_EVICTOR;

        Object objectValue = serializationService.toObject(value);

        return newRecord(mapConfig, perEntryStatsEnabled, hasEviction, objectValue);
    }

    @Nonnull
    private Record<Object> newRecord(MapConfig mapConfig, boolean perEntryStatsEnabled,
                                     boolean hasEviction, Object objectValue) {
        if (perEntryStatsEnabled) {
            return new ObjectRecordWithStats(objectValue);
        }

        if (hasEviction) {
            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.LRU) {
                return new SimpleRecordWithLRUEviction<>(objectValue);
            }

            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.LFU) {
                return new SimpleRecordWithLFUEviction<>(objectValue);
            }

            if (mapConfig.getEvictionConfig().getEvictionPolicy() == EvictionPolicy.RANDOM) {
                return new SimpleRecord<>(objectValue);
            }

            return new ObjectRecordWithStats(objectValue);
        }

        return new SimpleRecord<>(objectValue);
    }
}
