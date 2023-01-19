package com.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import static com.hazelcast.spi.properties.ClusterProperty.ALTO_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.ALTO_EVENTLOOP_COUNT;

/**
 * Demo application for TPC. Will be removed in in the final release.
 */
public class ClientMain {

    public static void main(String[] args) {
        System.setProperty(ALTO_ENABLED.getName(), "true");
        System.setProperty(ALTO_EVENTLOOP_COUNT.getName(), "" + Runtime.getRuntime().availableProcessors());
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        System.out.println("Client created");
        IMap<Integer, Integer> map = client.getMap("foo");

        long count = 4_000_000;
        long startTime = System.currentTimeMillis();

        for (int k = 0; k < count; k++) {
            if (k % 100000 == 0) {
                System.out.println("At:" + k);
            }
            map.put(k, k);
        }

        long duration = System.currentTimeMillis() - startTime;
        double throughput = count * 1000f / duration;
        System.out.println("Throughput:" + throughput + " op/s");
        System.exit(0);
    }
}
