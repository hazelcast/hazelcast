package com.hazelcast.internal.tpc.iobuffer;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Random;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.tpc.iobuffer.TpcIOBufferAllocator.INITIAL_POOL_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class TpcIOBufferTest {
    private final TpcIOBufferAllocator allocator = new TpcIOBufferAllocator();
    private final Random random = new Random();

    @Test
    public void when_allocatingNewBuffer() {
        allocator.allocate(TpcIOBufferAllocator.BUFFER_SIZE);
    }

    @Test
    public void when_overflowingBufferCache() {
        assert allocator.bufferPool.length == INITIAL_POOL_SIZE;

        TpcIOBuffer[] buffers = new TpcIOBuffer[INITIAL_POOL_SIZE + 1];
        for (int i = 0; i < INITIAL_POOL_SIZE + 1; i++) {
            buffers[i] = allocator.allocate();
        }
        for (int i = 0; i < buffers.length; i++) {
            allocator.free(buffers[i]);
        }

        assert allocator.bufferPool.length > INITIAL_POOL_SIZE;
    }

    @Test
    public void when_writingInt_then_valueCanBeRead() {
        TpcIOBuffer buffer = allocator.allocate();
        int randomInt = random.nextInt();
        buffer.writeInt(randomInt);
        buffer.writeIntL(randomInt);

        int randomIntL = buffer.getInt(INT_SIZE_IN_BYTES);
        buffer.writeIntL(randomIntL); // littleEndian(littleEndian) = bigEndian

        assertEquals(randomInt, buffer.getInt(0));
        assertNotEquals(randomInt, buffer.getInt(INT_SIZE_IN_BYTES)); // littleEndian != bigEndian
        assertEquals(randomInt, buffer.getInt(INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES)); // littleEndian(littleEndian) = bigEndian
    }
}