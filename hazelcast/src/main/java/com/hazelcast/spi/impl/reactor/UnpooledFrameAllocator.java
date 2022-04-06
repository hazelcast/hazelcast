package com.hazelcast.spi.impl.reactor;

public class UnpooledFrameAllocator implements FrameAllocator{

    public UnpooledFrameAllocator(int i) {

    }

    public UnpooledFrameAllocator(){}

    @Override
    public Frame allocate() {
        throw new RuntimeException();
    }

    @Override
    public Frame allocate(int minSize) {
        return new Frame(minSize);
    }

    @Override
    public void release(Frame frame) {
    }
}
