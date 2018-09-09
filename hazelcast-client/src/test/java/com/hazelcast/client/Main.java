package com.hazelcast.client;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {

    public static void main(String[] args){
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();


    }
}
