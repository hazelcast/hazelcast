package com.hazelcast.spi.impl.engine.frame;

public class UnpooledFrameAllocator implements FrameAllocator {

    public UnpooledFrameAllocator(int i) {

    }

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
