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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractRecordFactoryTest<T> extends HazelcastTestSupport {

    SerializationService serializationService;
    PartitioningStrategy partitioningStrategy;
    RecordFactory<T> factory;

    Person object1;
    Person object2;

    Data data1;
    Data data2;

    Record<T> record;

    @Before
    public final void init() {
        serializationService = createSerializationService();
        partitioningStrategy = new DefaultPartitioningStrategy();

        object1 = new Person("Alice");
        object2 = new Person("Bob");

        data1 = serializationService.toData(object1);
        data2 = serializationService.toData(object2);
    }

    @Test
    public void testNewRecord_withStatisticsDisabledAndCacheDeserializedValuesIsALWAYS() {
        newRecordFactory(false, CacheDeserializedValues.ALWAYS);
        record = newRecord(factory, object1);

        assertInstanceOf(getCachedRecordClass(), record);
    }

    @Test
    public void testNewRecord_withStatisticsDisabledAndCacheDeserializedValuesIsNEVER() {
        newRecordFactory(false, CacheDeserializedValues.NEVER);
        record = newRecord(factory, object1);

        assertInstanceOf(getRecordClass(), record);
    }

    @Test
    public void testNewRecord_withStatisticsEnabledAndCacheDeserializedValuesIsALWAYS() {
        newRecordFactory(true, CacheDeserializedValues.ALWAYS);
        record = newRecord(factory, object1);

        assertInstanceOf(getCachedRecordWithStatsClass(), record);
    }

    @Test
    public void testNewRecord_withStatisticsEnabledAndCacheDeserializedValuesIsNEVER() {
        newRecordFactory(true, CacheDeserializedValues.NEVER);
        record = newRecord(factory, object1);

        assertInstanceOf(getRecordWithStatsClass(), record);
    }


    abstract void newRecordFactory(boolean isStatisticsEnabled,
                                   CacheDeserializedValues cacheDeserializedValues);

    abstract Class<?> getRecordClass();

    abstract Class<?> getRecordWithStatsClass();

    abstract Class<?> getCachedRecordClass();

    abstract Class<?> getCachedRecordWithStatsClass();

    InternalSerializationService createSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    Record<T> newRecord(RecordFactory<T> factory, Object value) {
        return factory.newRecord(value);
    }
}
