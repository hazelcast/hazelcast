package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.util.ClientMessageAccumulator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * ClientMessageAccumulator Tests
 */
public class ClientMessageAccumulatorTest {


    private static final byte[] BYTE_DATA = new byte[] { 20, 0, 0, 0 ,0, 0, 0 ,0, 0, 0 ,0, 0, 0 ,0, 0, 0 ,0, 0, 0 ,0, 0, 0 };

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void shouldAccumulateClientMessageCorrectly() {
        ClientMessageAccumulator accumulator = new ClientMessageAccumulator();
        final ByteBuffer inBuffer = ByteBuffer.wrap(BYTE_DATA);
        accumulator.accumulate(inBuffer);
        final ByteBuffer byteBuffer = accumulator.accumulatedByteBuffer();
        assertEquals(0, byteBuffer.position());
        assertEquals(accumulator.getFrameLength(), byteBuffer.limit());

        for (int i = 0; i < byteBuffer.limit(); i++) {
            assertEquals(BYTE_DATA[i], byteBuffer.get());
        }
        assertTrue(accumulator.isComplete());
    }

    @Test
    public void shouldNotAccumulateInCompleteFrameSize() {
        ClientMessageAccumulator accumulator = new ClientMessageAccumulator();
        final byte[] array = new byte[]{1,2,3};
        final ByteBuffer inBuffer = ByteBuffer.wrap(array);
        final int accumulate = accumulator.accumulate(inBuffer);
        assertEquals(0, accumulate);
        assertFalse(accumulator.isComplete());
    }

}
