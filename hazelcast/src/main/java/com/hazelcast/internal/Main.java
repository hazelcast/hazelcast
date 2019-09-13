package com.hazelcast.internal;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class Main {

    public static void main(String[] args){
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        System.out.println("Connected");

        IMap map = hz1.getMap("foo");
        for(int k=0;k<100000;k++){
            map.get(k);
            if(k%1000==0){
                System.out.println("at:"+k);
            }
        }
        System.out.println("Done");
        System.exit(0);
    }
}

