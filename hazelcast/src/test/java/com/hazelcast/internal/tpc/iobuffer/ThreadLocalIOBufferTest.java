package com.hazelcast.internal.tpc.iobuffer;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Random;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.tpc.iobuffer.ThreadLocalIOBufferAllocator.BUFFER_SIZE;
import static com.hazelcast.internal.tpc.iobuffer.ThreadLocalIOBufferAllocator.INITIAL_POOL_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Category({QuickTest.class, ParallelJVMTest.class})
public class ThreadLocalIOBufferTest {
    private final ThreadLocalIOBufferAllocator allocator = new ThreadLocalIOBufferAllocator();
    private final Random random = new Random();

    @Test
    public void when_allocatingNewBuffer() {
        allocator.allocate(ThreadLocalIOBufferAllocator.BUFFER_SIZE);
    }

    @Test
    public void when_overflowingBufferCache() {
        assert allocator.byteBufferPool.length == INITIAL_POOL_SIZE;

        ThreadLocalIOBuffer[] buffers = new ThreadLocalIOBuffer[INITIAL_POOL_SIZE + 1];
        for (int i = 0; i < INITIAL_POOL_SIZE + 1; i++) {
            buffers[i] = allocator.allocate();
        }
        for (int i = 0; i < buffers.length; i++) {
            allocator.free(buffers[i]);
        }

        assert allocator.byteBufferPool.length > INITIAL_POOL_SIZE;
    }

    @Test
    public void when_writingInt_then_valueCanBeRead() {
        ThreadLocalIOBuffer buffer = allocator.allocate();
        int randomInt = random.nextInt();
        buffer.writeInt(randomInt);
        buffer.writeIntL(randomInt);

        int randomIntL = buffer.getInt(INT_SIZE_IN_BYTES);
        buffer.writeIntL(randomIntL); // littleEndian(littleEndian) = bigEndian

        assertEquals(randomInt, buffer.getInt(0));
        assertNotEquals(randomInt, buffer.getInt(INT_SIZE_IN_BYTES)); // littleEndian != bigEndian
        assertEquals(randomInt, buffer.getInt(INT_SIZE_IN_BYTES + INT_SIZE_IN_BYTES)); // littleEndian(littleEndian) = bigEndian
    }

    @Test
    public void when_writingSplittedInt_then_valueCanBeRead() {
        ThreadLocalIOBuffer buffer = allocator.allocate();
        int randomInt = random.nextInt();

        buffer.writeBytes(new byte[BUFFER_SIZE - 2]);
        buffer.writeInt(randomInt);

        assertEquals(randomInt, buffer.getInt(BUFFER_SIZE - 2));
    }

    @Test
    public void when_writingMultipleInts_then_properValuesAreRead() {
        ThreadLocalIOBuffer buffer = allocator.allocate();
        for (int i = 0; i < BUFFER_SIZE; i++) {
            buffer.writeInt(i);
        }

        for (int i = 0; i < BUFFER_SIZE; i++) {
            assertEquals(i, buffer.getInt(i * INT_SIZE_IN_BYTES));
        }
    }

    @Test
    public void when_bufferIsFreed_then_ioAndByteBuffersAreInPools() {
        assertEquals(0, allocator.byteBufferPoolPos);
        assertEquals(0, allocator.ioBufferPoolPos);

        assertNull(allocator.ioBufferPool[0]);
        assertNull(allocator.byteBufferPool[0]);

        ThreadLocalIOBuffer buffer = allocator.allocate(1);
        allocator.free(buffer);

        assertEquals(1, allocator.byteBufferPoolPos);
        assertEquals(1, allocator.ioBufferPoolPos);

        assertNotNull(allocator.ioBufferPool[0]);
        assertNotNull(allocator.byteBufferPool[0]);
    }
}