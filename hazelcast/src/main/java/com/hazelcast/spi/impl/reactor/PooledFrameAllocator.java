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
        private Frame[] frames = new Frame[32];
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
       // allocateCalls.incrementAndGet();

        //  System.out.println("allocate:release " + allocateCalls.get() + ":" + releaseCalls.get());

        //System.out.println("allocate called");

        Pool pool = POOL.get();
        if (pool == null) {
            pool = new Pool();
            POOL.set(pool);
        }

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
                frame.allocator = this;
                pool.index++;
                pool.frames[k] = frame;
            }
        }

      //  System.out.println("Allocation: pooled " + ((newAllocations.get() * 100.0f) / allocateCalls.get())+" %");

        Frame frame = pool.frames[pool.index];
        pool.frames[pool.index] = null;
        pool.index--;
        frame.acquire();
        return frame;
    }

    @Override
    public void release(Frame frame) {
        //   System.out.println("release called");

        //       releaseCalls.incrementAndGet();

        //    System.out.println("allocate:release " + allocateCalls.get() + ":" + releaseCalls.get());

        Pool pool = POOL.get();
        frame.clear();
        frame.next = null;
        frame.future = null;

        if (pool == null || pool.index == pool.frames.length - 1) {
            // if pool == null:
            // if the thread has never allocated anything, we don't want to create
            // a pool for that thread.

            // if  pool.index == pool.frames.length:
            // the pool of the object is full, so lets toss it in the queue.
            queue.offer(frame);
        } else {
            // there is space in the pool.
            pool.index++;
            pool.frames[pool.index] = frame;
        }
    }
}
