/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.Data;

public class DataRecordFactory implements RecordFactory<Data> {

    private final boolean statisticsEnabled;
    private final SerializationService ss;
    private final CacheDeserializedValues cacheDeserializedValues;

    public DataRecordFactory(MapConfig config, SerializationService ss) {
        this.ss = ss;
        this.statisticsEnabled = config.isStatisticsEnabled();
        this.cacheDeserializedValues = config.getCacheDeserializedValues();
    }

    @Override
    public Record<Data> newRecord(Object value) {
        Data valueData = ss.toData(value);

        switch (cacheDeserializedValues) {
            case NEVER:
                return statisticsEnabled ? new DataRecordWithStats(valueData) : new DataRecord(valueData);
            default:
                return statisticsEnabled ? new CachedDataRecordWithStats(valueData) : new CachedDataRecord(valueData);
        }
    }
}
