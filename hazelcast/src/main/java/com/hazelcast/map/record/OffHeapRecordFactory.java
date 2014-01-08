/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.record;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.storage.DataRef;
import com.hazelcast.storage.Storage;

/**
 * @author mdogan 10/3/13
 */
public class OffHeapRecordFactory implements RecordFactory<Data> {

    private final Storage<DataRef> storage;
    private final SerializationService serializationService;
    private final PartitioningStrategy partitionStrategy;
    private final boolean statisticsEnabled;

    public OffHeapRecordFactory(MapConfig config, Storage<DataRef> storage, SerializationService serializationService, PartitioningStrategy partitionStrategy) {
        this.storage = storage;
        this.serializationService = serializationService;
        this.partitionStrategy = partitionStrategy;
        this.statisticsEnabled = config.isStatisticsEnabled();
    }

    @Override
    public InMemoryFormat getStorageFormat() {
        return InMemoryFormat.OFFHEAP;
    }

    @Override
    public Record<Data> newRecord(Data key, Object value) {
        Data v = serializationService.toData(value, partitionStrategy);
        return new OffHeapRecord(storage, key, v, statisticsEnabled);
    }

    @Override
    public void setValue(Record<Data> record, Object value) {
        final Data v;
        if (value instanceof Data) {
            v = (Data) value;
        } else {
            v = serializationService.toData(value, partitionStrategy);
        }
        record.setValue(v);
    }

    @Override
    public boolean isEquals(Object value1, Object value2) {
        return serializationService.toData(value1).equals(serializationService.toData(value2));
    }
}
