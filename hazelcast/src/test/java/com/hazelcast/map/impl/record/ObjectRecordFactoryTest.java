/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ObjectRecordFactoryTest extends AbstractRecordFactoryTest<Object> {

    @Override
    void newRecordFactory(boolean isStatisticsEnabled,
                          CacheDeserializedValues cacheDeserializedValues) {
        MapContainer mapContainer = createMapContainer(isStatisticsEnabled, cacheDeserializedValues);
        factory = new ObjectRecordFactory(mapContainer, serializationService);
    }

    @Override
    Class<?> getRecordClass() {
        return SimpleRecord.class;
    }

    @Override
    Class<?> getRecordWithStatsClass() {
        return ObjectRecordWithStats.class;
    }

    @Override
    Class<?> getCachedRecordClass() {
        return SimpleRecord.class;
    }

    @Override
    Class<?> getCachedRecordWithStatsClass() {
        return ObjectRecordWithStats.class;
    }
}
