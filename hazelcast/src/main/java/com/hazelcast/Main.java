package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hazelcast.partition.count","1");
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        for(int k=0;k<1000;k++){
            hz1.getMap("map").put(k,k);
        }

        System.out.println(hz2.getMap("map").size());
        Thread.sleep(10000);
        System.exit(0);
    }
}
