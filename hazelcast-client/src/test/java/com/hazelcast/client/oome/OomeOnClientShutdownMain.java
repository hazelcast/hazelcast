package com.hazelcast.client.oome;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * A test application that starts and stops clients. There was a  bug in the system that eventually leads to
 * an OOME and this application was used to detect that bug. So run this application e.g. in combination with a profiler
 * and see how memory usage behaves.
 */
public class OomeOnClientShutdownMain {

    public static void main(String[] args) {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();

        for (int k = 0; k < 1000000; k++) {
            System.out.println("At:" + k);
            HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
            client.getLifecycleService().shutdown();
        }
    }
}
