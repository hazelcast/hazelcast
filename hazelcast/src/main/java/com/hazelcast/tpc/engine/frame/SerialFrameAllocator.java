package com.hazelcast.tpc.engine.frame;

import java.nio.ByteBuffer;

/**
 * A {@link FrameAllocator} that can only be used serially (so by a single thread).
 */
public final class SerialFrameAllocator implements FrameAllocator {
    private final int minSize;
    private final boolean direct;
    private long newAllocateCnt = 0;
    private long allocateCnt = 0;
    private Frame[] frames = new Frame[4096];
    private int index = -1;

    public SerialFrameAllocator(int minSize, boolean direct) {
        this.minSize = minSize;
        this.direct = direct;
    }

    @Override
    public Frame allocate() {
        allocateCnt++;

        if (index == -1) {
            // the pool is empty.
            // and lets create a set of frames so we don't end up
            // continuously asking the queue for requests.
            for (int k = 0; k < frames.length; k++) {
                //newAllocations.incrementAndGet();
                //System.out.println(" new frame");
                ByteBuffer buffer = direct ? ByteBuffer.allocateDirect(minSize) : ByteBuffer.allocate(minSize);
                Frame frame = new Frame(buffer);
                frame.concurrent = false;
                newAllocateCnt++;
                frame.allocator = this;
                index++;
                frames[k] = frame;
            }
        }

        if (allocateCnt % 10_000_000 == 0) {
            System.out.println("New allocate percentage:" + (newAllocateCnt * 100f) / allocateCnt + "%");
        }

        Frame frame = frames[index];
        frames[index] = null;
        index--;
        frame.acquire();
        return frame;
    }

    @Override
    public Frame allocate(int minSize) {
        Frame frame = allocate();
        frame.ensureRemaining(minSize);
        return frame;
    }

    @Override
    public void free(Frame frame) {
        frame.clear();
        frame.next = null;
        frame.future = null;

        if (index == frames.length - 1) {
            Frame[] newframes = new Frame[frames.length * 2];
            System.arraycopy(frames, 0, newframes, 0, frames.length);
            frames = newframes;
        }

        index++;
        frames[index] = frame;
    }
}
