package com.hazelcast.jclouds;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;

/**
 * Created by bilal on 7/25/16.
 */
public class Server {
    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type","log4j");
        HazelcastClient.newHazelcastClient();
    }
}
