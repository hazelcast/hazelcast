package com.hazelcast.test;

/**
 *  Interface to implement maximum threads calculation for each test separately
 *  Use
 *
 *  @TestProperties(maxThreadsCalculatorClass="YourThreadsAwareImpl")
 *  public class YourTest {
 *      
 *  }
 * */
public interface MaxThreadsAware {
    int maxThreads();
}
