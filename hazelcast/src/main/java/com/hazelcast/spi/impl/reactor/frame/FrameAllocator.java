package com.hazelcast.spi.impl.reactor;

public interface FrameAllocator {

    Frame allocate();

    Frame allocate(int minSize);

    void free(Frame frame);
}
