package com.hazelcast.tpc.engine.frame;

public interface FrameAllocator {

    Frame allocate();

    Frame allocate(int minSize);

    void free(Frame frame);
}
