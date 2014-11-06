package com.hazelcast.cache.jsr;

import com.hazelcast.core.Hazelcast;

import javax.cache.Caching;

/**
 * To clean resources after running a jsr test
 */
public class CleanupUtil {

    public static void cleanup() {
        Caching.getCachingProvider().close();
        Hazelcast.shutdownAll();
    }
}
