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

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractRecordComparatorTest extends HazelcastTestSupport {

    SerializationService serializationService;
    PartitioningStrategy partitioningStrategy;

    RecordComparator comparator;

    Person object1;
    Person object2;

    Data data1;
    Data data2;
    Data nullData;

    @Before
    public final void init() {
        serializationService = createSerializationService();
        partitioningStrategy = new DefaultPartitioningStrategy();

        object1 = new Person("Alice");
        object2 = new Person("Bob");

        data1 = serializationService.toData(object1);
        data2 = serializationService.toData(object2);
        nullData = new HeapData(new byte[0]);
    }

    @Test
    public void testIsEqual() {
        newRecordComparator();

        assertTrue(comparator.isEqual(null, null));
        assertTrue(comparator.isEqual(object1, object1));
        assertTrue(comparator.isEqual(object1, data1));
        assertTrue(comparator.isEqual(data1, data1));
        assertTrue(comparator.isEqual(data1, object1));
        assertTrue(comparator.isEqual(nullData, nullData));

        assertFalse(comparator.isEqual(null, object1));
        assertFalse(comparator.isEqual(null, data1));
        assertFalse(comparator.isEqual(null, nullData));
        assertFalse(comparator.isEqual(object1, null));
        assertFalse(comparator.isEqual(object1, nullData));
        assertFalse(comparator.isEqual(object1, object2));
        assertFalse(comparator.isEqual(object1, data2));
        assertFalse(comparator.isEqual(data1, null));
        assertFalse(comparator.isEqual(data1, nullData));
        assertFalse(comparator.isEqual(data1, object2));
        assertFalse(comparator.isEqual(data1, data2));
        assertFalse(comparator.isEqual(nullData, null));
        assertFalse(comparator.isEqual(nullData, object1));
        assertFalse(comparator.isEqual(nullData, data1));
    }

    @Test
    public void testIsEqual_withCustomPartitioningStrategy() {
        partitioningStrategy = new PersonPartitioningStrategy();

        data1 = serializationService.toData(object1, partitioningStrategy);
        data2 = serializationService.toData(object2, partitioningStrategy);

        testIsEqual();
    }

    abstract void newRecordComparator();

    InternalSerializationService createSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    static class PersonPartitioningStrategy implements PartitioningStrategy<Person> {

        @Override
        public Object getPartitionKey(Person key) {
            return key.name;
        }
    }
}
