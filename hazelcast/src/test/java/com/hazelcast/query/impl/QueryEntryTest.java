/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryEntryTest extends HazelcastTestSupport {

    protected InternalSerializationService serializationService;

    @Before
    public void before() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @After
    public void after() {
        serializationService.dispose();
    }

    // ========================== getAttribute ===========================================

    @Test
    public void getAttribute_whenValueIsPortableObject_thenConvertedToData() {
        Data key = serializationService.toData("indexedKey");
        Portable value = new SampleObjects.PortableEmployee(30, "peter");
        QueryableEntry queryEntry = entry(key, value, Extractors.empty());

        // in the portable-data, the attribute 'name' is called 'n'. So if we can retrieve on n
        // correctly it shows that we have used the Portable data, not the actual Portable object
        Object result = queryEntry.getAttributeValue("n");

        assertEquals("peter", result);
    }

    @Test
    public void getAttribute_whenKeyIsPortableObject_thenConvertedToData() {
        Data key = serializationService.toData(new SampleObjects.PortableEmployee(30, "peter"));
        SerializableObject value = new SerializableObject();
        QueryableEntry queryEntry = entry(key, value, Extractors.empty());

        // in the portable-data, the attribute 'name' is called 'n'. So if we can retrieve on n
        // correctly it shows that we have used the Portable data, not the actual Portable object
        Object result = queryEntry.getAttributeValue(QueryConstants.KEY_ATTRIBUTE_NAME.value() + ".n");

        assertEquals("peter", result);
    }

    @Test
    public void getAttribute_whenKeyPortableObjectThenConvertedToData() {
        Data key = serializationService.toData(new SampleObjects.PortableEmployee(30, "peter"));
        SerializableObject value = new SerializableObject();
        QueryableEntry queryEntry = entry(key, value, Extractors.empty());

        Object result = queryEntry.getAttributeValue(QueryConstants.KEY_ATTRIBUTE_NAME.value() + ".n");

        assertEquals("peter", result);
    }

    @Test
    public void getAttribute_whenValueInObjectFormatThenNoSerialization() {
        Data key = serializationService.toData(new SerializableObject());
        SerializableObject value = new SerializableObject();
        value.name = "somename";
        QueryableEntry queryEntry = entry(key, value, Extractors.empty());

        Object result = queryEntry.getAttributeValue("name");

        assertEquals("somename", result);
        assertEquals(0, value.deserializationCount);
        assertEquals(0, value.serializationCount);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInit_whenKeyIsNull_thenThrowIllegalArgumentException() {
        Data key = null;
        entry(key, new SerializableObject(), Extractors.empty());
    }

    @Test
    public void test_init() throws Exception {
        Data dataKey = serializationService.toData("dataKey");
        Data dataValue = serializationService.toData("dataValue");
        QueryableEntry queryEntry = entry(dataKey, dataValue, Extractors.empty());
        Object objectValue = queryEntry.getValue();
        Object objectKey = queryEntry.getKey();

        init(queryEntry, serializationService, serializationService.toData(objectKey), objectValue, Extractors.empty());

        // compare references of objects since they should be cloned after QueryEntry#init call.
        assertTrue("Old dataKey should not be here", dataKey != queryEntry.getKeyData());
        assertTrue("Old dataValue should not be here", dataValue != queryEntry.getValueData());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_init_nullKey() throws Exception {
        entry(null, "value", Extractors.empty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_setValue() throws Exception {
        QueryableEntry queryEntry = entry(mock(Data.class), "value", Extractors.empty());
        queryEntry.setValue("anyValue");
    }

    @Test(expected = NullPointerException.class)
    public void test_equality_empty() throws Exception {
        QueryableEntry entryKeyLeft = entry();
        QueryableEntry entryKeyRight = entry();

        entryKeyLeft.equals(entryKeyRight);
    }

    @Test
    public void test_equality_same() throws Exception {
        QueryableEntry entry = entry();

        assertTrue(entry.equals(entry));
    }

    @Test
    public void test_equality_differentType() throws Exception {
        QueryableEntry entry = entry();

        assertFalse(entry.equals("string"));
    }

    @Test
    public void test_equality_null() throws Exception {
        QueryableEntry entry = entry();

        assertFalse(entry.equals(null));
    }

    @Test
    public void test_equality_differentKey() throws Exception {
        QueryableEntry queryEntry = entry("dataKey", "dataValue");
        QueryableEntry queryEntryOther = entry("dataKeyOther", "dataValue");

        assertFalse(queryEntry.equals(queryEntryOther));
    }

    @Test
    public void test_equality_sameKey() throws Exception {
        QueryableEntry queryEntry = entry("dataKey", "dataValue");
        QueryableEntry queryEntryOther = entry("dataKey", "dataValueOther");

        assertTrue(queryEntry.equals(queryEntryOther));
    }

    @Test
    public void getKey_caching() {
        QueryableEntry entry = entry("key", "value");

        assertThat(entry.getKey(), not(sameInstance(entry.getKey())));
    }

    @Test
    public void getValue_caching() {
        QueryableEntry entry = entry("key", "value");

        assertThat(entry.getValue(), not(sameInstance(entry.getValue())));
    }

    @Test
    public void getKeyData_caching() {
        QueryableEntry entry = entry("key", "value");

        assertThat(entry.getKeyData(), sameInstance(entry.getKeyData()));
    }

    @Test
    public void getValueData_caching() {
        QueryableEntry entry = entry("key", "value");

        assertThat(entry.getValueData(), sameInstance(entry.getValueData()));
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

    protected QueryableEntry entry(String key, String value) {
        return entry(serializationService.toData(key),
                serializationService.toData(value),
                Extractors.empty());
    }

    protected QueryableEntry entry(Data key, Object value, Extractors extractors) {
        return new QueryEntry(serializationService, key, value, extractors);
    }

    protected QueryableEntry entry() {
        return new QueryEntry();
    }

    protected void init(Object entry, InternalSerializationService serializationService, Data key, Object value, Extractors extractors) {
        ((QueryEntry) entry).init(serializationService, key, value, extractors);
    }

}
