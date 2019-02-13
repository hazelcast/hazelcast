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
import com.hazelcast.map.impl.MetadataInitializer;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DataRecordFactoryTest extends AbstractRecordFactoryTest<Data> {

    @Override
    void newRecordFactory(boolean isStatisticsEnabled, CacheDeserializedValues cacheDeserializedValues,
                          MetadataInitializer metadataInitializer) {
        MapConfig mapConfig = new MapConfig()
                .setStatisticsEnabled(isStatisticsEnabled)
                .setCacheDeserializedValues(cacheDeserializedValues);

        factory = new DataRecordFactory(mapConfig, serializationService, partitioningStrategy, metadataInitializer);
    }

    @Test
    public void testMetadataInitializerIsCalledAtCreation() {
        newRecordFactory(false, CacheDeserializedValues.ALWAYS, metadataInitializer);
        newRecord(factory, data1, object1);

        assertEquals(2, metadataInitializer.getDataCall());
        assertEquals(0, metadataInitializer.getObjectCall());
    }

    @Test
    public void testMetadataInitializerIsCalledAtUpdate() {
        newRecordFactory(false, CacheDeserializedValues.ALWAYS, metadataInitializer);
        record = newRecord(factory, data1, object1);

        factory.setValue(record, object2);

        assertEquals(3, metadataInitializer.getDataCall());
        assertEquals(0, metadataInitializer.getObjectCall());
    }

    @Test
    public void testMetadataIsCreatedLaterWhenValueIsUpdatedAndItHasMetadataFirstTime() {
        newRecordFactory(false, CacheDeserializedValues.ALWAYS, new MetadataInitializer() {
            @Override
            public Object createFromData(Data keyData) throws IOException {
                return keyData == data2 ? true : null;
            }

            @Override
            public Object createFromObject(Object object) throws IOException {
                return null;
            }
        });
        record = factory.newRecord(data1, data1);

        factory.setValue(record, data2);

        assertNotNull(record.getMetadata());
        assertTrue((Boolean) record.getMetadata().getValueMetadata());
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
