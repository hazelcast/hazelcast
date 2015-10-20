package com.hazelcast.client.cache.jsr;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;

import javax.cache.Cache;
import javax.cache.Caching;

/**
 * utility class responsible for setup/cleanup client jsr tests
 */
public class JsrClientTestUtil {

    static {
        System.setProperty("javax.management.builder.initial", "com.hazelcast.cache.impl.TCKMBeanServerBuilder");
        System.setProperty("org.jsr107.tck.management.agentId", "TCKMbeanServer");
        System.setProperty(Cache.class.getName(), "com.hazelcast.cache.ICache");
        System.setProperty(Cache.Entry.class.getCanonicalName(), "com.hazelcast.cache.impl.CacheEntry");
    }

    public static void setup() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        Hazelcast.newHazelcastInstance(config);
    }

    public static void cleanup() {
        Caching.getCachingProvider().close();
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }
}