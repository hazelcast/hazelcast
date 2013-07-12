package com.hazelcast.client.oome;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

/**
 * A test application that starts clients with bad credentials. There was a  bug in the system that eventually
 * leads to an OOME and this application was used to detect that bug. So run this application e.g. in combination with
 * a profiler and see how memory usage behaves.
 */
public class OomeOnClientAuthenticationMain {

    public static void main(String[] args) {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setPassword("foo");
        clientConfig.setConnectionAttemptLimit(0);

        for (int k = 0; k < 1000000; k++) {
            System.out.println("At:" + k);
            try {
                HazelcastClient.newHazelcastClient(clientConfig);
            } catch (IllegalStateException e) {

            }
        }
    }
}
