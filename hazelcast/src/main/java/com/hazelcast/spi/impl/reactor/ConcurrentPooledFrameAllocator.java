package com.hazelcast.spi.impl.reactor;


import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpmcArrayQueue;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentPooledFrameAllocator implements FrameAllocator {

    private final MpmcArrayQueue<Frame> queue = new MpmcArrayQueue<>(4096);

    private static final AtomicLong newAllocations = new AtomicLong(0);
    private static final AtomicLong pooledAllocations = new AtomicLong(0);
    private static final AtomicLong allocateCalls = new AtomicLong();
    private static final AtomicLong releaseCalls = new AtomicLong();
    private final boolean direct;

    static class Pool {
        private long newAllocateCnt = 0;
        private long allocateCnt = 0;
        private Frame[] frames = new Frame[128];
        private int index = -1;
        private final MessagePassingQueue.Consumer<Frame> consumer = frame -> {
            index++;
            frames[index] = frame;
        };
    }

    private final static ThreadLocal<Pool> POOL = new ThreadLocal<>();
    private final int minSize;

    public ConcurrentPooledFrameAllocator(int minSize, boolean direct) {
        this.minSize = minSize;
        this.direct = direct;
    }

    @Override
    public Frame allocate(int minSize) {
        //todo
        return allocate();
    }

    @Override
    public Frame allocate() {
        Pool pool = POOL.get();
        if (pool == null) {
            pool = new Pool();
            POOL.set(pool);
        }
        pool.allocateCnt++;
        if (pool.index == -1) {
            // the pool is empty.

            int count = 0;
            for (int k = 0; k < pool.frames.length; k++) {
                Frame frame = queue.poll();
                if (frame == null) {
                    break;
                }
                count++;
                pool.index++;
                pool.frames[pool.index] = frame;
            }

            // Lets gets some frames from the queue.
            //int count = queue.drain(pool.consumer, pool.frames.length);

            // and lets create a bunch of frames ourselves so we don't end up
            // continuously asking the queue for requests.
            for (int k = count; k < pool.frames.length; k++) {
                //newAllocations.incrementAndGet();
                //System.out.println(" new frame");
                ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(minSize) : ByteBuffer.allocate(minSize);
                Frame frame = new Frame(buffer);
                frame.concurrent = true;
                frame.allocator = this;
                pool.newAllocateCnt++;
                pool.index++;
                pool.frames[k] = frame;
            }
        }

        if (pool.allocateCnt % 1_000_000 == 0) {
            System.out.println("New allocate percentage:" + (pool.newAllocateCnt * 100f) / pool.allocateCnt + "%");
        }

        Frame frame = pool.frames[pool.index];
        pool.frames[pool.index] = null;
        pool.index--;

        // for debugging; it should not be needed. We should be able to do a lazySet 1
        for (; ; ) {
            int c = frame.refCount.get();
            if (c != 0) {
                throw new RuntimeException("Ref count should be 0, but was: " + frame.refCount());
            }

            if (frame.refCount.compareAndSet(0, 1)) {
                break;
            }
        }
        return frame;
    }

    @Override
    public void free(Frame frame) {
        if (frame.refCount.get() != 0) {
            throw new RuntimeException("Frame refCount should be 0, but was:" + frame.refCount.get());
        }

        frame.clear();
        frame.next = null;
        frame.future = null;

        Pool pool = POOL.get();
        if (pool == null) {
            // if pool == null:
            // if the thread has never allocated anything, we don't want to create
            // a pool for that thread.
            queue.offer(frame);
            return;
        }

        if (pool.index == pool.frames.length - 1) {
            // the pool of the object is full, so lets toss it in the queue.
            queue.offer(frame);
        } else {
            // there is space in the pool.
            pool.index++;
            pool.frames[pool.index] = frame;
        }
    }
}
