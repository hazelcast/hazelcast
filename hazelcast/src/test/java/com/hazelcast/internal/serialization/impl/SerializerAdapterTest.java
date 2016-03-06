package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteOrder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SerializerAdapterTest {

    private SerializationService mockSerializationService;

    @Before
    public void setUp() {
        mockSerializationService = mock(SerializationService.class);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor_whenNullSerializer() {
        new SerializerAdapter(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor_whenUnrecognizedSerializer() {
        new SerializerAdapter(new UnrecognizedSerializer());
    }

    static class UnrecognizedSerializer implements Serializer {
        @Override
        public int getTypeId() {
            return 0;
        }

        @Override
        public void destroy() {
        }
    }

    @Test
    public void testConstructor_whenByteArraySerializer() {
        ByteArraySerializer byteArraySerializer = mock(ByteArraySerializer.class);
        when(byteArraySerializer.getTypeId()).thenReturn(10);

        SerializerAdapter serializerAdapter = new SerializerAdapter(byteArraySerializer);

        assertEquals(10, serializerAdapter.getTypeId());
        assertSame(byteArraySerializer, serializerAdapter.getImpl());
    }

    @Test
    public void testConstructor_whenStreamSerializer() {
        StreamSerializer streamSerializer = mock(StreamSerializer.class);
        when(streamSerializer.getTypeId()).thenReturn(10);

        SerializerAdapter serializerAdapter = new SerializerAdapter(streamSerializer);

        assertEquals(10, serializerAdapter.getTypeId());
        assertSame(streamSerializer, serializerAdapter.getImpl());
    }

    @Test
    public void testSerializationDeserialization_whenByteArraySerializer() throws IOException {
        ByteArraySerializerImpl byteArraySerializer = new ByteArraySerializerImpl();
        SerializerAdapter serializerAdapter = new SerializerAdapter(byteArraySerializer);

        byte[] testIn = new byte[]{(byte)1, (byte)2, (byte)3};

        ByteArrayObjectDataOutput out = new ByteArrayObjectDataOutput(100, mockSerializationService, ByteOrder.BIG_ENDIAN);
        ByteArrayObjectDataInput in = new ByteArrayObjectDataInput(out.buffer, mockSerializationService, ByteOrder.BIG_ENDIAN);
        serializerAdapter.write(out, testIn);

        byte[] read = (byte[])serializerAdapter.read(in);

        assertArrayEquals(testIn, read);
    }

    public static class ByteArraySerializerImpl implements ByteArraySerializer {
        @Override
        public byte[] write(Object object) throws IOException {
            return (byte[])object;
        }

        @Override
        public Object read(byte[] buffer) throws IOException {
            return buffer;
        }

        @Override
        public int getTypeId() {
            return 0;
        }

        @Override
        public void destroy() {
        }
    }

    @Test
    public void testSerializationDeserialization_whenStreamSerializer() throws Exception {
        SerializerAdapter adapter = new SerializerAdapter(new ConstantSerializers.IntegerArraySerializer());

        int[] testIn = new int[]{1, 2, 3};

        ByteArrayObjectDataOutput out = new ByteArrayObjectDataOutput(100, mockSerializationService, ByteOrder.BIG_ENDIAN);
        ByteArrayObjectDataInput in = new ByteArrayObjectDataInput(out.buffer, mockSerializationService, ByteOrder.BIG_ENDIAN);
        adapter.write(out, testIn);
        int[] read = (int[]) adapter.read(in);

        assertArrayEquals(testIn, read);
    }

    @Test
    public void testAdaptorEqualAndHashCode() throws Exception {
        SerializerAdapter adapter = new SerializerAdapter(new ConstantSerializers.IntegerArraySerializer());
        SerializerAdapter theOther = new SerializerAdapter(adapter.getImpl());

        // todo: equals is not perfect because if a 2 adapters would be created with new IntegerArraySerializer for example,
        // they would return false because equality is done on instance check.

        assertEquals(adapter, adapter);
        assertEquals(adapter, theOther);
        assertNotEquals(adapter, null);
        assertNotEquals(adapter, "Not An Adaptor");

        assertEquals(adapter.hashCode(), adapter.getImpl().hashCode());
    }

    @Test
    public void testString() throws Exception {
        SerializerAdapter adapter = new SerializerAdapter(new ConstantSerializers.IntegerArraySerializer());
        assertNotNull(adapter.toString());
    }
}
