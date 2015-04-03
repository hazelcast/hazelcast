package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
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
        ClientMessage accumulator = ClientMessage.create();
        final ByteBuffer inBuffer = ByteBuffer.wrap(BYTE_DATA);
        accumulator.readFrom(inBuffer);

        final ByteBuffer byteBuffer = accumulatedByteBuffer(accumulator.buffer().byteBuffer(), accumulator.index());
        assertEquals(0, byteBuffer.position());
        assertEquals(accumulator.getFrameLength(), byteBuffer.limit());

        for (int i = 0; i < byteBuffer.limit(); i++) {
            assertEquals(BYTE_DATA[i], byteBuffer.get());
        }
        assertTrue(accumulator.isComplete());
    }

    @Test
    public void shouldNotAccumulateInCompleteFrameSize() {
        ClientMessage accumulator = ClientMessage.create();
        final byte[] array = new byte[]{1,2,3};
        final ByteBuffer inBuffer = ByteBuffer.wrap(array);
        assertFalse(accumulator.readFrom(inBuffer));
        assertFalse(accumulator.isComplete());
    }

    /**
     * setup the wrapped bytebuffer to point to this clientMessages data
     * @return
     */
    public ByteBuffer accumulatedByteBuffer(final ByteBuffer byteBuffer, int index) {
        if (byteBuffer != null) {
            byteBuffer.limit(index);
            byteBuffer.position(index);
            byteBuffer.flip();
            return byteBuffer;
        }
        return null;
    }

}
