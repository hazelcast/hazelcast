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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Data;

public class ObjectRecordFactory implements RecordFactory<Object> {

    private final SerializationService serializationService;
    private final boolean statisticsEnabled;

    public ObjectRecordFactory(MapConfig config,
                               SerializationService serializationService) {
        this.serializationService = serializationService;
        this.statisticsEnabled = config.isStatisticsEnabled();
    }

    @Override
    public Record<Object> newRecord(Data key, Object value) {
        assert value != null : "value can not be null";

        Object objectValue = serializationService.toObject(value);

        Record<Object> record = statisticsEnabled
                ? new ObjectRecordWithStats(objectValue)
                : new ObjectRecord(objectValue);

        record.setKey(key);

        return record;
    }
}
