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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * @author mdogan 10/3/13
 */
public class ObjectRecordFactory implements RecordFactory<Object> {

    private final SerializationService serializationService;
    private final boolean statisticsEnabled;

    public ObjectRecordFactory(MapConfig config, SerializationService serializationService) {
        this.serializationService = serializationService;
        this.statisticsEnabled = config.isStatisticsEnabled();
    }

    @Override
    public InMemoryFormat getStorageFormat() {
        return InMemoryFormat.OBJECT;
    }

    @Override
    public Record<Object> newRecord(Data key, Object value) {
        Object v = value;
        if (value instanceof Data) {
            v = serializationService.toObject((Data) value);
        }
        return new ObjectRecord(key, v, statisticsEnabled);
    }

    @Override
    public void setValue(Record<Object> record, Object value) {
        Object v = value;
        if (value instanceof Data) {
            v = serializationService.toObject((Data) value);
        }
        record.setValue(v);
    }

    @Override
    public boolean isEquals(Object value1, Object value2) {
        Object v1 = value1 instanceof Data ? serializationService.toObject((Data) value1) : value1;
        Object v2 = value2 instanceof Data ? serializationService.toObject((Data) value2) : value2;
        if (v1 == null && v2 == null) {
            return true;
        }
        if (v1 == null) {
            return false;
        }
        if (v2 == null) {
            return false;
        }
        return v1.equals(v2);
    }
}
