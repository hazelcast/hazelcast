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

import com.hazelcast.config.MapConfig;
import com.hazelcast.map.impl.MetadataInitializer;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Metadata;
import com.hazelcast.spi.serialization.SerializationService;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class ObjectRecordFactory implements RecordFactory<Object> {

    private final SerializationService serializationService;
    private final boolean statisticsEnabled;
    private final MetadataInitializer metadataInitializer;

    public ObjectRecordFactory(MapConfig config,
                               SerializationService serializationService,
                               MetadataInitializer metadataInitializer) {
        this.serializationService = serializationService;
        this.statisticsEnabled = config.isStatisticsEnabled();
        this.metadataInitializer = metadataInitializer;
    }

    @Override
    public Record<Object> newRecord(Data key, Object value) {
        assert value != null : "value can not be null";

        Object objectValue = serializationService.toObject(value);
        Record<Object> record = statisticsEnabled ? new ObjectRecordWithStats(objectValue) : new ObjectRecord(objectValue);
        record.setMetadata(initializeMetadata(key, objectValue));
        return record;
    }

    @Override
    public void setValue(Record<Object> record, Object value) {
        assert value != null : "value can not be null";

        Object v = value;
        if (value instanceof Data) {
            v = serializationService.toObject(value);
        }
        record.setValue(v);
        updateValueMetadata(record, v);
    }

    private void updateValueMetadata(Record<Object> record, Object value) {
        try {
            Object valueMetadata = metadataInitializer.createFromObject(value);
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

    private Metadata initializeMetadata(Data key, Object value) {

        try {
            Object keyMetadata = metadataInitializer.createFromData(key);
            Object valueMetadata = metadataInitializer.createFromObject(value);
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
