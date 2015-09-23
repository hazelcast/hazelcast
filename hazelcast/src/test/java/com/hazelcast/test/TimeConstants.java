package com.hazelcast.test;

/**
 * Constants in milliseconds for use in test annotations such as @Test(timeout = 5*SECOND)
 */
public interface TimeConstants {

    long SECOND = 1000;

    long MINUTE = 60 * SECOND;
}
