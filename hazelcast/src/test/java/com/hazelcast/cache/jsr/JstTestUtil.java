package com.hazelcast.cache.jsr;

import com.hazelcast.core.Hazelcast;

import javax.cache.Caching;

/**
 * utility class responsible for cleanup jsr tests
 */
public class JstTestUtil {

    public static void cleanup() {
        Caching.getCachingProvider().close();
        Hazelcast.shutdownAll();
    }
}
