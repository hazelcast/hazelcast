package com.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class ClientMain {

    public static void main(String[] args) {
        System.setProperty("reactor.enabled", "true");
        System.setProperty("reactor.count", "1");
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        System.out.println("Client created");
        IMap map = client.getMap("foo");

        long count = 2_000_000;
        long startTime = System.currentTimeMillis();

        for (int k = 0; k < count; k++) {
            if(k % 100000 == 0){
                System.out.println("At:"+k);
            }
            map.put(0, k);
        }

        long duration = System.currentTimeMillis()-startTime;
        double throughput = count * 1000f/duration;
        System.out.println("Throughput:"+throughput+" op/s");
        System.exit(0);
    }

}
