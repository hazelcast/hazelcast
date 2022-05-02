package com.hazelcast.tpc.offheapmap;

public interface Query {
    void process(long address);

    void clear();
}
