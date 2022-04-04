package com.hazelcast.spi.impl.reactor;

public class PooledFrameAllocator implements FrameAllocator {

    private final Thread owner;
    private final int minSize;
    private Frame[] frames = new Frame[128];
    private int index = -1;
    private long allocateCalls;
    private long poolHits;

    public PooledFrameAllocator(Thread owner, int minSize) {
        this.owner = owner;
        this.minSize = minSize;
    }

    @Override
    public Frame allocate() {
        allocateCalls++;
        if (index < 0) {
            Frame frame = new Frame(minSize);
            frame.allocator = this;
            return frame;
        }

        poolHits++;
        Frame frame = frames[index];
        index--;
        return frame;
    }

    @Override
    public void release(Frame frame) {
        if (owner != Thread.currentThread()) {
            return;
        }

        if (index == frames.length - 1) {
            return;
        }
        frame.clear();
        index++;
        frames[index] = frame;
    }

    public String toString() {
        if (allocateCalls == 0) {
            return "PooledFrameAllocator(hit-ratio:na)";
        }
        return "PooledFrameAllocator(hit-ratio:" + (poolHits * 100f / allocateCalls) + "%)";
    }
}
