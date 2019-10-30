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

package com.hazelcast.map.impl.record;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.MapConfig;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;

public class DataRecordFactory implements RecordFactory<Data> {

    private final SerializationService serializationService;
    private final PartitioningStrategy partitionStrategy;
    private final CacheDeserializedValues cacheDeserializedValues;
    private final boolean statisticsEnabled;

    public DataRecordFactory(MapConfig config, SerializationService serializationService,
                             PartitioningStrategy partitionStrategy) {
        this.serializationService = serializationService;
        this.partitionStrategy = partitionStrategy;
        this.statisticsEnabled = config.isStatisticsEnabled();
        this.cacheDeserializedValues = config.getCacheDeserializedValues();
    }

    @Override
    public Record<Data> newRecord(Data key, Object value) {
        assert value != null : "value can not be null";

        final Data valueData = serializationService.toData(value, partitionStrategy);
        Record<Data> record;
        switch (cacheDeserializedValues) {
            case NEVER:
                record = statisticsEnabled ? new DataRecordWithStats(valueData) : new DataRecord(valueData);
                break;
            default:
                record = statisticsEnabled ? new CachedDataRecordWithStats(valueData) : new CachedDataRecord(valueData);
        }
        record.setKey(key);
        return record;
    }
}
