package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MultiMap;

import java.util.Map;

public class Main {

    public static void main(String[] args) {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
        clientConfig.getNetworkConfig().setSmartRouting(false);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        System.out.println("Connected");


//
        Map<Object, Object> map = client.getMap("foo");
        System.out.println("===============================================");
        System.out.println(map.get("bar"));
        System.out.println("===============================================");
        System.exit(0);
    }
}
