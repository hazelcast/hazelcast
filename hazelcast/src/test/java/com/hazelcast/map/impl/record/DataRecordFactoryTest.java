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

package com.hazelcast.map.impl.record;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DataRecordFactoryTest extends HazelcastTestSupport {

    private SerializationService mockSerializationService;
    private PartitioningStrategy mockPartitioningStrategy;

    private Object object = new Object();
    private Data data = new HeapData();

    @Before
    public void setUp() {
        mockSerializationService = mock(SerializationService.class);
        mockPartitioningStrategy = mock(PartitioningStrategy.class);
        object = new Object();
        data = new HeapData();
        when(mockSerializationService.toData(object, mockPartitioningStrategy)).thenReturn(data);
    }

    @Test
    public void givenStatisticsEnabledAndCacheDeserializedValuesIsNEVER_thenCreateDataRecordWithStats() {
        MapConfig mapConfig = new MapConfig().setStatisticsEnabled(true).setCacheDeserializedValues(CacheDeserializedValues.NEVER);
        DataRecordFactory dataRecordFactory = new DataRecordFactory(mapConfig, mockSerializationService, mockPartitioningStrategy);

        Record<Data> dataRecord = newDataRecord(dataRecordFactory);

        assertInstanceOf(DataRecordWithStats.class, dataRecord);
    }

    @Test
    public void givenStatisticsDisabledAndCacheDeserializedValuesIsNEVER_thenCreateDataRecordWithStats() {
        MapConfig mapConfig = new MapConfig().setStatisticsEnabled(false).setCacheDeserializedValues(CacheDeserializedValues.NEVER);
        DataRecordFactory dataRecordFactory = new DataRecordFactory(mapConfig, mockSerializationService, mockPartitioningStrategy);

        Record<Data> dataRecord = newDataRecord(dataRecordFactory);

        assertInstanceOf(DataRecord.class, dataRecord);
    }

    @Test
    public void givenStatisticsEnabledAndCacheDeserializedValuesIsDefault_thenCreateCachedDataRecordWithStats() {
        MapConfig mapConfig = new MapConfig().setStatisticsEnabled(true);
        DataRecordFactory dataRecordFactory = new DataRecordFactory(mapConfig, mockSerializationService, mockPartitioningStrategy);

        Record<Data> dataRecord = newDataRecord(dataRecordFactory);

        assertInstanceOf(CachedDataRecordWithStats.class, dataRecord);
    }

    @Test
    public void givenStatisticsDisabledAndCacheDeserializedValuesIsDefault_thenCreateCachedDataRecord() {
        MapConfig mapConfig = new MapConfig().setStatisticsEnabled(false);
        DataRecordFactory dataRecordFactory = new DataRecordFactory(mapConfig, mockSerializationService, mockPartitioningStrategy);

        Record<Data> dataRecord = newDataRecord(dataRecordFactory);

        assertInstanceOf(CachedDataRecord.class, dataRecord);
    }

    private Record<Data> newDataRecord(DataRecordFactory dataRecordFactory) {
        Record<Data> record = dataRecordFactory.newRecord(object);
        ((AbstractRecord) record).setKey(data);
        return record;
    }
}
