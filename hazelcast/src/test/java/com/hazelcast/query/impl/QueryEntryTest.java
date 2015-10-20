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
