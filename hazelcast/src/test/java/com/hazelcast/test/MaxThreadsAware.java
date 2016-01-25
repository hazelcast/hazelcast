package com.hazelcast.test;

/**
 * Interface to implement maximum threads calculation for each test separately.
 * <p/>
 * Usage:
 * <pre>
 * @TestProperties(maxThreadsCalculatorClass="YourThreadsAwareImpl")
 * public class YourTest {
 * }
 * </pre>
 */
public interface MaxThreadsAware {

    int maxThreads();
}
