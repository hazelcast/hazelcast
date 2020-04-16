package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {

    public static void main(String[] args){
        System.out.println("========= Starting first node");
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();

        System.out.println("========= Starting second node");
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        System.out.println("========== Connected");

        for(int k=0;k<1000;k++){
            System.out.println("At:"+k);
            hz1.getMap("foo").get(k);
        }

        System.out.println("done");
        System.exit(0);
    }
}
