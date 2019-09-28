package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {

    public static void main(String[] args){
        System.setProperty("ioCpus","2-10");

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

    }
}
