package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {
    public static void main(String[] args){
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        System.out.println("done");

        for(int k=0;k<1000;k++){
            hz1.getMap("foo").set(k,k);
            System.out.println(hz2.getMap("foo").get(k));
        }
    }
}
