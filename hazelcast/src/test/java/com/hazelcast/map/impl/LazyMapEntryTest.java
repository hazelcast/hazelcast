package com.hazelcast.map.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LazyMapEntryTest extends HazelcastTestSupport {

    private LazyMapEntry entry = new LazyMapEntry();
    private SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Test
    public void test_init() throws Exception {
        Data keyData = serializationService.toData("keyData");
        Data valueData = serializationService.toData("valueData");
        entry.init(keyData, valueData, serializationService);

        Object valueObject = entry.getValue();
        Object keyObject = entry.getKey();

        entry.init(keyObject, valueObject, serializationService);

        assertTrue("Old keyData should not be here", keyData != entry.getKeyData());
        assertTrue("Old valueData should not be here", valueData != entry.getValueData());
    }

    @Test
    public void test_init_doesNotSerializeObject() throws Exception {
        MyObject key = new MyObject();
        MyObject value = new MyObject();

        entry.init(key, value, serializationService);

        assertEquals(0, key.serializedCount);
        assertEquals(0, value.serializedCount);
    }

    @Test
    public void test_init_doesNotDeserializeObject() throws Exception {
        MyObject keyObject = new MyObject();
        MyObject valueObject = new MyObject();

        Data keyData = serializationService.toData(keyObject);
        Data valueData = serializationService.toData(valueObject);

        entry.init(keyData, valueData, serializationService);

        assertEquals(1, keyObject.serializedCount);
        assertEquals(1, valueObject.serializedCount);
        assertEquals(0, keyObject.deserializedCount);
        assertEquals(0, valueObject.deserializedCount);
    }


    @Test
    public void testLazyDeserializationWorks() throws Exception {
        MyObject keyObject = new MyObject();
        MyObject valueObject = new MyObject();

        Data keyData = serializationService.toData(keyObject);
        Data valueData = serializationService.toData(valueObject);

        entry.init(keyData, valueData, serializationService);

        Object key = entry.getKey();
        Object value = entry.getValue();

        assertInstanceOf(MyObject.class, key);
        assertInstanceOf(MyObject.class, value);
        assertEquals(1, ((MyObject) key).deserializedCount);
        assertEquals(1, ((MyObject) value).deserializedCount);
    }

    private static class MyObject implements DataSerializable, Serializable {

        int serializedCount = 0;
        int deserializedCount = 0;

        public MyObject() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(++serializedCount);
            out.writeInt(deserializedCount);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            serializedCount = in.readInt();
            deserializedCount = in.readInt() + 1;
        }

        @Override
        public String toString() {
            return "MyObject{"
                    + "deserializedCount=" + deserializedCount
                    + ", serializedCount=" + serializedCount
                    + '}';
        }
    }

}