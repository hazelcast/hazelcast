package com.hazelcast.test.annotation;

import com.hazelcast.test.ParallelRunnerOptions;

/**
 * Limits a number of tests running in parallel for heavily multi-threaded tests.
 */
public class HeavilyMultiThreadedTestLimiter implements ParallelRunnerOptions {

    @Override
    public int maxParallelTests() {
        return Math.max(Runtime.getRuntime().availableProcessors() / 2, 1);
    }

}
