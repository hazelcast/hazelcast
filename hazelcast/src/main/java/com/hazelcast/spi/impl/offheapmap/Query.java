package com.hazelcast.spi.impl.offheapmap;

public interface Query {
    void process(long address);

    void clear();
}
