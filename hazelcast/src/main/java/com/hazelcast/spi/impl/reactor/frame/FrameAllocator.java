package com.hazelcast.spi.impl.reactor.frame;

public interface FrameAllocator {

    Frame allocate();

    Frame allocate(int minSize);

    void free(Frame frame);
}
