package com.hazelcast.spi.impl.reactor;

public interface FrameAllocator {

    Frame allocate();

    void release(Frame frame);
}
