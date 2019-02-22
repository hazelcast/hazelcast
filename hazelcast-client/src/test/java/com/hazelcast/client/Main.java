package com.hazelcast.client;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;

public class Main {
    public static void main(String[] args){
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = Hazelcast.newHazelcastInstance(new Config().setLiteMember(true));
        //HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IAtomicLong l = client.getAtomicLong("foo");
        System.out.println(l.get());
    }
}
