package com.hazelcast.mapreduce;

import com.hazelcast.test.MaxThreadsAware;

public class MapReduceTestThreads implements MaxThreadsAware {
    @Override
    public int maxThreads() {
        return Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
    }
}