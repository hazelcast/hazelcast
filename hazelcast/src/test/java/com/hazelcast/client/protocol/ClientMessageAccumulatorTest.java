package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.MutableDirectBuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * ClientMessageAccumulator Tests
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
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

        final ByteBuffer byteBuffer = accumulatedByteBuffer(accumulator.buffer(), accumulator.index());
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
    static ByteBuffer accumulatedByteBuffer(final MutableDirectBuffer buffer, int index) {
        if (buffer != null) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer.byteArray());
            byteBuffer.limit(index);
            byteBuffer.position(index);
            byteBuffer.flip();
            return byteBuffer;
        }
        return null;
    }

}
