package com.hazelcast.internal.serialization.impl;

import com.hazelcast.nio.serialization.SerializationV1Dataserializable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EmptyObjectDataOutputTest {

    private EmptyObjectDataOutput out;
    @Before
    public void setUp() {
        out = new EmptyObjectDataOutput();
    }

    @Test
    public void testEmptyMethodsDoNothing() throws IOException {
        ByteOrder byteOrder = out.getByteOrder();
        out.close();
        assertEquals(ByteOrder.BIG_ENDIAN, byteOrder);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testToByteArray() throws IOException {
        out.toByteArray();
    }

}
