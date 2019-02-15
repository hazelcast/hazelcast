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
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.map.impl.MetadataInitializer;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Metadata;
import com.hazelcast.spi.serialization.SerializationService;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class DataRecordFactory implements RecordFactory<Data> {

    private final SerializationService serializationService;
    private final PartitioningStrategy partitionStrategy;
    private final CacheDeserializedValues cacheDeserializedValues;
    private final MetadataInitializer metadataInitializer;
    private final boolean statisticsEnabled;

    public DataRecordFactory(MapConfig config, SerializationService serializationService,
                             PartitioningStrategy partitionStrategy, MetadataInitializer metadataInitializer) {
        this.serializationService = serializationService;
        this.partitionStrategy = partitionStrategy;
        this.statisticsEnabled = config.isStatisticsEnabled();
        this.cacheDeserializedValues = config.getCacheDeserializedValues();
        this.metadataInitializer = metadataInitializer;
    }

    @Override
    public Record<Data> newRecord(Data key, Object value) {
        assert value != null : "value can not be null";

        final Data data = serializationService.toData(value, partitionStrategy);
        Record<Data> record;
        switch (cacheDeserializedValues) {
            case NEVER:
                record = statisticsEnabled ? new DataRecordWithStats(data) : new DataRecord(data);
                break;
            default:
                record = statisticsEnabled ? new CachedDataRecordWithStats(data) : new CachedDataRecord(data);
        }
        record.setKey(key);
        record.setMetadata(initializeMetadata(key, data));
        return record;
    }

    @Override
    public void setValue(Record<Data> record, Object value) {
        assert value != null : "value can not be null";

        final Data v;
        if (value instanceof Data) {
            v = (Data) value;
        } else {
            v = serializationService.toData(value, partitionStrategy);
        }
        record.setValue(v);
        updateValueMetadata(record, v);
    }

    private void updateValueMetadata(Record<Data> record, Data dataValue) {
        try {
            Object valueMetadata = metadataInitializer.createFromData(dataValue);
            if (valueMetadata != null) {
                Metadata existing = record.getMetadata();
                if (existing == null) {
                    existing = new Metadata();
                    record.setMetadata(existing);
                }
                existing.setValueMetadata(valueMetadata);
            }
        } catch (Exception e) {
            rethrow(e);
        }
    }

    private Metadata initializeMetadata(Data key, Data value) {

        try {
            Object keyMetadata = metadataInitializer.createFromData(key);
            Object valueMetadata = metadataInitializer.createFromData(value);
            if (keyMetadata != null || valueMetadata != null) {
                Metadata metadata = new Metadata();
                metadata.setKeyMetadata(keyMetadata);
                metadata.setValueMetadata(valueMetadata);
                return metadata;
            }
        } catch (Exception e) {
            rethrow(e);
        }
        return null;
    }
}
