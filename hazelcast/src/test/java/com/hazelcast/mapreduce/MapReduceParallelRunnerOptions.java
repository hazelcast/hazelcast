package com.hazelcast.mapreduce;

import com.hazelcast.test.ParallelRunnerOptions;

public class MapReduceParallelRunnerOptions implements ParallelRunnerOptions {
    @Override
    public int maxParallelTests() {
        return Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
    }
}