package com.hazelcast.spi.impl.reactor;

public class PooledFrameAllocator implements FrameAllocator {

    private final Thread owner;
    private final int minSize;
    private Frame[] frames = new Frame[128];
    private int index = -1;

    public PooledFrameAllocator(Thread owner, int minSize) {
        this.owner = owner;
        this.minSize = minSize;
    }

    @Override
    public Frame allocate() {
        if (index < 0) {
            Frame frame = new Frame(minSize);
            frame.allocator = this;
            return frame;
        }

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
}
