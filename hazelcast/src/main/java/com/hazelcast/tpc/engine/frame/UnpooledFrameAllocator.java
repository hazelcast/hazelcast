package com.hazelcast.tpc.engine.frame;

/**
 * A {@link FrameAllocator} that doesn't do any pooling of requests.
 */
public final class UnpooledFrameAllocator implements FrameAllocator {

    public UnpooledFrameAllocator() {
    }

    @Override
    public Frame allocate() {
        throw new RuntimeException();
    }

    @Override
    public Frame allocate(int minSize) {
        return new Frame(minSize);
    }

    @Override
    public void free(Frame frame) {
    }
}
