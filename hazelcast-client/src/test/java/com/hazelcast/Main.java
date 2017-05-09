package com.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 *
 * todo:
 * - metrics are not registered.
 * - closing
 * - spinning
 * - packet reader doesn't have correct packet counters
 * - client message reader doesn't have correct packet counters
 * - textmessage
 * - TcpIpConnector setProtocol is commented out.
 * - connection types are not set.
 * - overloaded connections plugin
 *
 * done
 * - publish in NioEventLoopGroup not working
 * - different protocols
 * - overloaded connections plugin working for niochannel
 */
public class Main {

    public static void main(String[] args) {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = HazelcastClient.newHazelcastClient();

        IMap map = hz2.getMap("foo");
        for (int k = 0; k < 10; k++) {
            map.put(k, k);
            System.out.println(map.get(k));
        }
        System.out.println("done");
    }
}
