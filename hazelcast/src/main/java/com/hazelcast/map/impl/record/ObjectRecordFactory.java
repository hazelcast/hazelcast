/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

public class ObjectRecordFactory implements RecordFactory<Object> {

    private final SerializationService serializationService;
    private final boolean statisticsEnabled;

    public ObjectRecordFactory(MapConfig config, SerializationService serializationService) {
        this.serializationService = serializationService;
        this.statisticsEnabled = config.isStatisticsEnabled();
    }

    @Override
    public Record<Object> newRecord(Object value) {
        assert value != null : "value can not be null";

        Object objectValue = serializationService.toObject(value);
        return statisticsEnabled ? new ObjectRecordWithStats(objectValue) : new ObjectRecord(objectValue);
    }

    @Override
    public void setValue(Record<Object> record, Object value) {
        assert value != null : "value can not be null";

        Object v = value;
        if (value instanceof Data) {
            v = serializationService.toObject(value);
        }
        record.setValue(v);
    }

    @Override
    public boolean isEquals(Object value1, Object value2) {
        if (value1 == value2) {
            return true;
        }
        if (value1 == null || value2 == null) {
            return false;
        }
        Object v1 = value1 instanceof Data ? serializationService.toObject(value1) : value1;
        Object v2 = value2 instanceof Data ? serializationService.toObject(value2) : value2;
        return v1 != null ? v1.equals(v2) : v2 == null;
    }
}
