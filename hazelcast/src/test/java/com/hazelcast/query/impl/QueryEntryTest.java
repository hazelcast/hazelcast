/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.query.QueryConstants;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryEntryTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void before() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @After
    public void after() {
        serializationService.destroy();
    }

    // ========================== getAttribute ===========================================

    @Test
    public void getAttribute_whenValueIsPortableObject_thenConvertedToData() {
        Data key = serializationService.toData("indexedKey");

        Portable value = new SampleObjects.PortableEmployee(30, "peter");
        QueryEntry queryEntry = new QueryEntry(serializationService, key, value, Extractors.empty());

        // in the portable-data, the attribute 'name' is called 'n'. So if we can retrieve on n
        // correctly it shows that we have used the Portable data, not the actual Portable object
        Object result = queryEntry.getAttributeValue("n");

        assertEquals("peter", result);
    }

    @Test
    public void getAttribute_whenKeyIsPortableObject_thenConvertedToData() {
        Data key = serializationService.toData(new SampleObjects.PortableEmployee(30, "peter"));

        SerializableObject value = new SerializableObject();
        QueryEntry queryEntry = new QueryEntry(serializationService, key, value, Extractors.empty());

        // in the portable-data, the attribute 'name' is called 'n'. So if we can retrieve on n
        // correctly it shows that we have used the Portable data, not the actual Portable object
        Object result = queryEntry.getAttributeValue(QueryConstants.KEY_ATTRIBUTE_NAME.value() + ".n");

        assertEquals("peter", result);
    }

    @Test
    public void getAttribute_whenKeyPortableObjectThenConvertedToData() {
        Data key = serializationService.toData(new SampleObjects.PortableEmployee(30, "peter"));

        SerializableObject value = new SerializableObject();
        QueryEntry queryEntry = new QueryEntry(serializationService, key, value, Extractors.empty());

        Object result = queryEntry.getAttributeValue(QueryConstants.KEY_ATTRIBUTE_NAME.value() + ".n");

        assertEquals("peter", result);
    }

    @Test
    public void getAttribute_whenValueInObjectFormatThenNoSerialization() {
        Data key = serializationService.toData(new SerializableObject());
        SerializableObject value = new SerializableObject();
        value.name = "somename";
        QueryEntry queryEntry = new QueryEntry(serializationService, key, value, Extractors.empty());

        Object result = queryEntry.getAttributeValue("name");

        assertEquals("somename", result);
        assertEquals(0, value.deserializationCount);
        assertEquals(0, value.serializationCount);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void givenNewQueryEntry_whenSetValue_thenThrowUnsupportedOperationException() {
        //given
        QueryEntry entry = newEntry();

        //when
        entry.setValue(new Object());
    }

    @Test
    public void testEquals_givenSameInstance_thenReturnTrue() {
        QueryEntry entry1 = newEntry();
        QueryEntry entry2 = entry1;

        assertTrue(entry1.equals(entry2));
    }

    @Test
    public void testEquals_givenOtherIsNull_thenReturnFalse() {
        QueryEntry entry = newEntry();

        assertFalse(entry.equals(null));
    }

    @Test
    public void testEquals_givenOtherIsDifferentClass_thenReturnFalse() {
        QueryEntry entry = newEntry();

        assertFalse(entry.equals(new Object()));
    }

    @Test
    public void testEquals_givenOtherHasDifferentKey_thenReturnFalse() {
        SerializableObject value = new SerializableObject();
        QueryEntry entry1 = newEntry("key1", value);
        QueryEntry entry2 = newEntry("key2", value);

        assertFalse(entry1.equals(entry2));
    }

    @Test
    public void testEquals_givenOtherHasEqualsKey_thenReturnTrue() {
        SerializableObject value = new SerializableObject();
        QueryEntry entry1 = newEntry("key", value);
        QueryEntry entry2 = newEntry("key", value);

        assertTrue(entry1.equals(entry2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInit_whenKeyIsNull_thenThrowIllegalArgumentException() {
        Data key = null;
        new QueryEntry(serializationService, key, new SerializableObject(), Extractors.empty());
    }

    @Test
    public void test_init() throws Exception {
        Data dataKey = serializationService.toData("dataKey");
        Data dataValue = serializationService.toData("dataValue");
        QueryEntry queryEntry = new QueryEntry(serializationService, dataKey, dataValue, Extractors.empty());

        Object objectValue = queryEntry.getValue();
        Object objectKey = queryEntry.getKey();

        queryEntry.init(serializationService, serializationService.toData(objectKey), objectValue, Extractors.empty());

        // compare references of objects since they should be cloned after QueryEntry#init call.
        assertTrue("Old dataKey should not be here", dataKey != queryEntry.getKeyData());
        assertTrue("Old dataValue should not be here", dataValue != queryEntry.getValueData());
    }

    private QueryEntry newEntry() {
        Data key = serializationService.toData(new SerializableObject());
        SerializableObject value = new SerializableObject();
        return new QueryEntry(serializationService, key, value, Extractors.empty());
    }

    private QueryEntry newEntry(Object key, Object value) {
        key = serializationService.toData(key);
        return new QueryEntry(serializationService, (Data)key, value, Extractors.empty());
    }

    @SuppressWarnings("unused")
    private static class SerializableObject implements DataSerializable {

        private int serializationCount;
        private int deserializationCount;
        private String name;

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            serializationCount++;
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            deserializationCount++;
        }
    }
}
