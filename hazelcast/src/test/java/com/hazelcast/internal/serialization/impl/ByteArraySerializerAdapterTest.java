package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ByteArraySerializerAdapterTest {

    private ByteArraySerializerAdapter adapter;
    private ConstantSerializers.TheByteArraySerializer serializer;

    private SerializationService mockSerializationService;

    @Before
    public void setUp() {
        mockSerializationService = mock(SerializationService.class);
        serializer = new ConstantSerializers.TheByteArraySerializer();
        adapter = new ByteArraySerializerAdapter(serializer);
    }

    @After
    public void tearDown() throws Exception {
        adapter.destroy();
    }

    @Test
    public void testAdaptor() throws Exception {
        byte[] testByteArray = new byte[]{1, 2, 3};

        ByteArrayObjectDataOutput out = new ByteArrayObjectDataOutput(10, mockSerializationService, ByteOrder.BIG_ENDIAN);
        ByteArrayObjectDataInput in = new ByteArrayObjectDataInput(out.buffer, mockSerializationService, ByteOrder.BIG_ENDIAN);
        adapter.write(out, testByteArray);
        byte[] read = (byte[]) adapter.read(in);

        Serializer impl = adapter.getImpl();

        assertArrayEquals(testByteArray, read);
        assertEquals(serializer, impl);
        assertEquals(serializer.getTypeId(), adapter.getTypeId());
    }

    @Test
    public void testAdaptorEqualAndHashCode() throws Exception {
        ByteArraySerializerAdapter theOther = new ByteArraySerializerAdapter(serializer);
        ByteArraySerializerAdapter theEmptyOne = new ByteArraySerializerAdapter(null);

        assertEquals(adapter, adapter);
        assertEquals(adapter, theOther);
        assertNotEquals(adapter, null);
        assertNotEquals(adapter, "Not An Adaptor");
        assertNotEquals(adapter, theEmptyOne);

        assertEquals(adapter.hashCode(), serializer.hashCode());

        assertEquals(0, theEmptyOne.hashCode());
    }

    @Test
    public void testString() throws Exception {
        assertNotNull(adapter.toString());
    }
}
