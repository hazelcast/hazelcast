package com.hazelcast.test;


/**
 * Options for configuring {@link HazelcastParallelClassRunner}
 */
public interface ParallelRunnerOptions {

    /**
     * @return the maximum number of tests the runner will run in parallel
     */
    int maxParallelTests();
}
