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
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
        record = newRecord(factory, data1, object1);

        assertInstanceOf(getCachedRecordClass(), record);
    }

    @Test
    public void testNewRecord_withStatisticsDisabledAndCacheDeserializedValuesIsNEVER() {
        newRecordFactory(false, CacheDeserializedValues.NEVER);
        record = newRecord(factory, data1, object1);

        assertInstanceOf(getRecordClass(), record);
    }

    @Test
    public void testNewRecord_withStatisticsEnabledAndCacheDeserializedValuesIsALWAYS() {
        newRecordFactory(true, CacheDeserializedValues.ALWAYS);
        record = newRecord(factory, data1, object1);

        assertInstanceOf(getCachedRecordWithStatsClass(), record);
    }

    @Test
    public void testNewRecord_withStatisticsEnabledAndCacheDeserializedValuesIsNEVER() {
        newRecordFactory(true, CacheDeserializedValues.NEVER);
        record = newRecord(factory, data1, object1);

        assertInstanceOf(getRecordWithStatsClass(), record);
    }

    @Test(expected = AssertionError.class)
    public void testNewRecord_withNullValue() {
        newRecordFactory(false, CacheDeserializedValues.ALWAYS);

        newRecord(factory, data1, null);
    }

    @Test
    public void testSetValue() {
        newRecordFactory(false, CacheDeserializedValues.ALWAYS);
        record = factory.newRecord(object1);

        factory.setValue(record, object2);

        assertEquals(getValue(data2, object2), record.getValue());
    }

    @Test
    public void testSetValue_withData() {
        newRecordFactory(false, CacheDeserializedValues.ALWAYS);
        record = factory.newRecord(object1);

        factory.setValue(record, data2);

        assertEquals(getValue(data2, object2), record.getValue());
    }

    @Test(expected = AssertionError.class)
    public void testSetValue_withNull() {
        newRecordFactory(false, CacheDeserializedValues.ALWAYS);
        record = factory.newRecord(object1);

        factory.setValue(record, null);
    }

    abstract void newRecordFactory(boolean isStatisticsEnabled, CacheDeserializedValues cacheDeserializedValues);

    abstract Class<?> getRecordClass();

    abstract Class<?> getRecordWithStatsClass();

    abstract Class<?> getCachedRecordClass();

    abstract Class<?> getCachedRecordWithStatsClass();

    abstract Object getValue(Data dataValue, Object objectValue);

    InternalSerializationService createSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    Record<T> newRecord(RecordFactory<T> factory, Data key, Object value) {
        Record<T> record = factory.newRecord(value);
        ((AbstractRecord) record).setKey(key);
        return record;
    }
}
