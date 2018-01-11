/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DataRecordFactoryTest extends AbstractRecordFactoryTest<Data> {

    @Override
    void newRecordFactory(boolean isStatisticsEnabled, CacheDeserializedValues cacheDeserializedValues) {
        MapConfig mapConfig = new MapConfig()
                .setStatisticsEnabled(isStatisticsEnabled)
                .setCacheDeserializedValues(cacheDeserializedValues);

        factory = new DataRecordFactory(mapConfig, serializationService, partitioningStrategy);
    }

    @Override
    Class<?> getRecordClass() {
        return DataRecord.class;
    }

    @Override
    Class<?> getRecordWithStatsClass() {
        return DataRecordWithStats.class;
    }

    @Override
    Class<?> getCachedRecordClass() {
        return CachedDataRecord.class;
    }

    @Override
    Class<?> getCachedRecordWithStatsClass() {
        return CachedDataRecordWithStats.class;
    }

    @Override
    Object getValue(Data dataValue, Object objectValue) {
        return dataValue;
    }
}
