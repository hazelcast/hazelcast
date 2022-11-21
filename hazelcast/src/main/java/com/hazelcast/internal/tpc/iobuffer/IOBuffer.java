package com.hazelcast.internal.tpc.iobuffer;

public interface IOBuffer {
    void release();

    int position();

    byte getByte(int pos);
}
