/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.core.EntryView;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.RecordStatistics;
import com.hazelcast.util.SampleableConcurrentHashMap;


/**
 * Internally used {@link EntryView} implementation for sampling based eviction specific purposes.
 *
 * Mainly :
 * - Wraps a {@link Record} and reaches all {@link EntryView} specific info over it
 * - Lazily de-serializes key and value.
 *
 * @param <R> Type of record to construct {@link EntryView} over it.
 */
public class LazyEntryViewFromRecord<R extends Record> extends SampleableConcurrentHashMap.SamplingEntry implements EntryView {

    private Object key;
    private Object value;
    private R record;

    private SerializationService serializationService;

    public LazyEntryViewFromRecord(R record, SerializationService serializationService) {
        super(record.getKey(), record);
        this.record = record;
        this.serializationService = serializationService;
    }

    @Override
    public Object getKey() {
        if (key == null) {
            key = serializationService.toObject(record.getKey());
        }
        return key;
    }

    @Override
    public Object getValue() {
        if (value == null) {
            value = serializationService.toObject(record.getValue());
        }
        return value;
    }

    @Override
    public long getCost() {
        return record.getCost();
    }

    @Override
    public long getCreationTime() {
        return record.getCreationTime();
    }

    @Override
    public long getExpirationTime() {
        RecordStatistics statistics = record.getStatistics();
        return statistics == null ? -1L : statistics.getExpirationTime();
    }

    @Override
    public long getHits() {
        return record.getHits();
    }

    @Override
    public long getLastAccessTime() {
        return record.getLastAccessTime();
    }

    @Override
    public long getLastStoredTime() {
        RecordStatistics statistics = record.getStatistics();
        return statistics == null ? -1L : statistics.getLastStoredTime();
    }

    @Override
    public long getLastUpdateTime() {
        return record.getLastUpdateTime();
    }

    @Override
    public long getVersion() {
        return record.getVersion();
    }

    @Override
    public long getTtl() {
        return record.getTtl();
    }

    public Record getRecord() {
        return record;
    }
}
