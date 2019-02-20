package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {

    public static final int COUNT = 100000;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.io.balancer.interval.seconds","0");
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        for (int k = 0; k < COUNT; k++) {
            hz1.getMap("foo").setAsync(k, k);
            // System.out.println(hz2.getMap("foo").get(k));
        }

        while (hz2.getMap("foo").size() != COUNT) {
            Thread.sleep(100);
            System.out.println("Map.size:"+hz2.getMap("foo").size());
        }

        System.out.println("done");
        System.exit(0);

    }
}
