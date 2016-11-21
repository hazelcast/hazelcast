package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ByteArrayObjectDataInputIntegrationTest {
    private final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testNotNull() throws Exception {
        readDataAsObject("foo");
    }

    @Test
    public void testNull() throws Exception {
        readDataAsObject(null);
    }

    public void readDataAsObject(Object value) throws Exception {
        Data data = serializationService.toData(value);
        MyObject myObject = new MyObject(data);
        Data myObjectData = serializationService.toData(myObject);
        MyObject myObjectDeserialized = serializationService.toObject(myObjectData);

        assertEquals(value, myObjectDeserialized.o);
    }

    private static class MyObject implements DataSerializable {
        private Data data;
        private Object o;

        public MyObject() {
        }

        public MyObject(Data data) {
            this.data = data;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeData(data);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            o = in.readDataAsObject();
        }
    }
}
