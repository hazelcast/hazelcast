package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.serialization.ByteArraySerializer;
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
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ByteArraySerializerStreamSerializerAdapterTest {

    private ByteArraySerializerStreamSerializerAdapter adapter;
    private ConstantSerializers.TheByteArraySerializer theByteArraySerializer;
    private SerializationService serializationService;

    @Before
    public void setUp() {
        serializationService = mock(SerializationService.class);
        theByteArraySerializer = new ConstantSerializers.TheByteArraySerializer();
        adapter = new ByteArraySerializerStreamSerializerAdapter(theByteArraySerializer);
    }

    @After
    public void tearDown() throws Exception {
        adapter.destroy();
    }

    @Test
    public void testAdaptor() throws Exception {
        byte[] testByteArray = new byte[]{1, 2, 3};

        ByteArrayObjectDataOutput out = new ByteArrayObjectDataOutput(10, serializationService, ByteOrder.BIG_ENDIAN);
        ByteArrayObjectDataInput in = new ByteArrayObjectDataInput(out.buffer, serializationService, ByteOrder.BIG_ENDIAN);
        adapter.write(out, testByteArray);
        byte[] read = (byte[]) adapter.read(in);

        assertArrayEquals(testByteArray, read);
        assertEquals(theByteArraySerializer.getTypeId(), adapter.getTypeId());
    }

    @Test
    public void testAdaptorEqualAndHashCode() throws Exception {
        ByteArraySerializerStreamSerializerAdapter theOther = new ByteArraySerializerStreamSerializerAdapter(
                theByteArraySerializer);
        ByteArraySerializerStreamSerializerAdapter theEmptyOne = new ByteArraySerializerStreamSerializerAdapter(
                mock(ByteArraySerializer.class));

        assertEquals(adapter, adapter);
        assertEquals(adapter, theOther);
        assertNotEquals(adapter, "Not An Adaptor");
        assertNotEquals(adapter, theEmptyOne);

        assertEquals(adapter.hashCode(), theByteArraySerializer.hashCode());
    }

    @Test
    public void testString() throws Exception {
        assertNotNull(adapter.toString());
    }
}
