package com.hazelcast.internal.tpc.iobuffer;

import org.jctools.queues.MpmcArrayQueue;

import java.nio.ByteBuffer;

import static com.hazelcast.internal.tpc.iobuffer.ThreadLocalIOBufferAllocator.INITIAL_POOL_SIZE;

class ConcurrentIOBufferAllocator implements IOBufferAllocator<ThreadLocalIOBuffer> {
    private static final ThreadLocal<ThreadLocalIOBufferAllocator> THREAD_LOCAL_IO_BUFFER_ALLOCATORS
            = new ThreadLocal<>();

    private static final MpmcArrayQueue<ThreadLocalIOBuffer> FREED_IO_BUFFERS = new MpmcArrayQueue<>(INITIAL_POOL_SIZE);
    private static final MpmcArrayQueue<ByteBuffer> FREED_BYTE_BUFFERS = new MpmcArrayQueue<>(INITIAL_POOL_SIZE);

    @Override
    public ThreadLocalIOBuffer allocate() {
        return allocate(DEFAULT_IO_BUFFER_SIZE);
    }

    @Override
    public ThreadLocalIOBuffer allocate(int minSize) {
        ThreadLocalIOBufferAllocator allocator = THREAD_LOCAL_IO_BUFFER_ALLOCATORS.get();
        if (allocator == null) {
            allocator = (ThreadLocalIOBufferAllocator) IOBufferAllocatorFactory.createNotGrowingThreadLocal(256, this);
            THREAD_LOCAL_IO_BUFFER_ALLOCATORS.set(allocator);
        }

        ThreadLocalIOBuffer oldIOBuffer;
        while (allocator.hasSpaceForIOBuffer() && (oldIOBuffer = FREED_IO_BUFFERS.poll()) != null) {
            allocator.freeExternalWithoutByteBuffers(oldIOBuffer);
        }

        ByteBuffer oldByteBuffer;
        while (allocator.hasSpaceForByteBuffer() && (oldByteBuffer = FREED_BYTE_BUFFERS.poll()) != null) {
            allocator.free(oldByteBuffer);
        }

        return allocator.allocate(minSize);
    }

    @Override
    public void free(ThreadLocalIOBuffer ioBuffer) {
        ThreadLocalIOBufferAllocator allocator = THREAD_LOCAL_IO_BUFFER_ALLOCATORS.get();

        int chunkToRelease = ioBuffer.chunkToRelease;
        boolean ioBufferFreed = false;

        if (allocator != null) {
            if (allocator.hasSpaceForIOBuffer()) {
                allocator.freeExternalWithoutByteBuffers(ioBuffer);
                ioBufferFreed = true;
            }

            for (; chunkToRelease < ioBuffer.chunks.length; chunkToRelease++) {
                if (!allocator.hasSpaceForByteBuffer()) {
                    break;
                }
                allocator.free(ioBuffer.chunks[chunkToRelease]);
            }
        }

        if (!ioBufferFreed) {
            FREED_IO_BUFFERS.offer(ioBuffer);
        }

        for (; chunkToRelease < ioBuffer.chunks.length; chunkToRelease++) {
            ByteBuffer chunk = ioBuffer.chunks[chunkToRelease];
            if (!FREED_BYTE_BUFFERS.offer(chunk)) {
                return;
            }
        }
    }

    @Override
    public void free(ByteBuffer chunk) {
        ThreadLocalIOBufferAllocator allocator = THREAD_LOCAL_IO_BUFFER_ALLOCATORS.get();
        if (allocator != null && allocator.hasSpaceForByteBuffer()) {
            allocator.free(chunk);
            return;
        }
        FREED_BYTE_BUFFERS.offer(chunk);
    }
}
