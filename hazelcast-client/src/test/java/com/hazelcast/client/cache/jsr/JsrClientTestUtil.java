package com.hazelcast.client.cache.jsr;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;

import javax.cache.Caching;

/**
 * utility class responsible for setup/cleanup client jsr tests
 */
public class JsrClientTestUtil {

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