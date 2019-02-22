package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class Main {
    public static void main(String[] args){
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        System.out.println("inserting too map");

        IMap map = hz1.getMap("foo");
        for(int k=0;k<20;k++){
            map.put(k,k);
        }

        System.out.println("done");
        System.exit(0);
    }

}
