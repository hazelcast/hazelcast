package com.hazelcast.spi.impl.reactor;


import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscArrayQueue;

import java.util.concurrent.atomic.AtomicLong;

public class PooledFrameAllocator implements FrameAllocator {

    private final MpscArrayQueue<Frame> queue = new MpscArrayQueue<>(4096);

    private static final AtomicLong newAllocations = new AtomicLong(0);
    private static final AtomicLong pooledAllocations = new AtomicLong(0);
    private static final AtomicLong allocateCalls = new AtomicLong();
    private static final AtomicLong releaseCalls = new AtomicLong();

    static class Pool {
        private long newAllocates = 0;
        private long allocates = 0;
        private Frame[] frames = new Frame[4096];
        private int index = -1;
        private final MessagePassingQueue.Consumer<Frame> consumer = frame -> {
            index++;
            frames[index] = frame;
        };
    }

    private final static ThreadLocal<Pool> POOL = new ThreadLocal<>();
    private final int minSize;

    public PooledFrameAllocator(int minSize) {
        this.minSize = minSize;
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
        pool.allocates++;
        if (pool.index == -1) {
            // the pool is empty.

            // Lets gets some frames from the queue.
            int count = queue.drain(pool.consumer, pool.frames.length);

            // and lets create a bunch of frames ourselves so we don't end up
            // continuously asking the queue for requests.
            for (int k = count; k < pool.frames.length; k++) {
                //newAllocations.incrementAndGet();
                //System.out.println(" new frame");
                Frame frame = new Frame(minSize);
                pool.newAllocates++;
                frame.allocator = this;
                pool.index++;
                pool.frames[k] = frame;
            }
        }

        if (pool.allocates % 10_000_000 == 0) {
            System.out.println("New allocate percentage:" + (pool.newAllocates * 100f) / pool.allocates + "%");
        }

        Frame frame = pool.frames[pool.index];
        pool.frames[pool.index] = null;
        pool.index--;
        frame.acquire();
        return frame;
    }

    @Override
    public void release(Frame frame) {
        frame.clear();
        frame.next = null;
        frame.future = null;

        Pool pool = POOL.get();
        if(pool == null){
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
