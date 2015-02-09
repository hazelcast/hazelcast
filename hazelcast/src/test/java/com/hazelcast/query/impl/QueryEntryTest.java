package com.hazelcast.query.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.QueryConstants;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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
        Data indexedKey = serializationService.toData("indexedKey");

        SerializableObject key = new SerializableObject();
        Portable value = new SampleObjects.PortableEmployee(30, "peter");
        QueryEntry queryEntry = new QueryEntry(serializationService, indexedKey, key, value);

        // in the portable-data, the attribute 'name' is called 'n'. So if we can retrieve on n
        // correctly it shows that we have used the Portable data, not the actual Portable object
        Object result = queryEntry.getAttribute("n");

        assertEquals("peter", result);
    }

    @Test
    public void getAttribute_whenKeyIsPortableObject_thenConvertedToData() {
        Data indexedKey = serializationService.toData("indexedKey");

        Portable key = new SampleObjects.PortableEmployee(30, "peter");
        SerializableObject value = new SerializableObject();
        QueryEntry queryEntry = new QueryEntry(serializationService, indexedKey, key, value);

        // in the portable-data, the attribute 'name' is called 'n'. So if we can retrieve on n
        // correctly it shows that we have used the Portable data, not the actual Portable object
        Object result = queryEntry.getAttribute(QueryConstants.KEY_ATTRIBUTE_NAME + ".n");

        assertEquals("peter", result);
    }

    @Test
    public void getAttribute_whenKeyPortableObjectThenConvertedToData() {
        Data indexedKey = serializationService.toData("indexedKey");

        Portable key = new SampleObjects.PortableEmployee(30, "peter");
        SerializableObject value = new SerializableObject();
        QueryEntry queryEntry = new QueryEntry(serializationService, indexedKey, key, value);

        Object result = queryEntry.getAttribute(QueryConstants.KEY_ATTRIBUTE_NAME + ".n");

        assertEquals("peter", result);
    }

    @Test
    public void getAttribute_whenValueInObjectFormatThenNoSerialization() {
        Data indexedKey = serializationService.toData("indexedKey");

        SerializableObject key = new SerializableObject();
        SerializableObject value = new SerializableObject();
        value.name = "somename";
        QueryEntry queryEntry = new QueryEntry(serializationService, indexedKey, key, value);

        Object result = queryEntry.getAttribute("name");

        assertEquals("somename", result);
        assertEquals(0, value.deserializationCount);
        assertEquals(0, value.serializationCount);
        assertEquals(0, key.deserializationCount);
        assertEquals(0, key.serializationCount);
    }

    @Test
    public void getAttribute_whenKeyInObjectFormatThenNoSerialization() {
        Data indexedKey = serializationService.toData("indexedKey");

        SerializableObject key = new SerializableObject();
        SerializableObject value = new SerializableObject();
        value.name = "somename";
        key.name = "somekey";
        QueryEntry queryEntry = new QueryEntry(serializationService, indexedKey, key, value);

        Object result = queryEntry.getAttribute(QueryConstants.KEY_ATTRIBUTE_NAME + ".name");

        assertEquals(result, "somekey");
        assertEquals(0, value.deserializationCount);
        assertEquals(0, value.serializationCount);
        assertEquals(0, key.deserializationCount);
        assertEquals(0, key.serializationCount);
    }

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
