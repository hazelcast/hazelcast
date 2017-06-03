package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * Created by alarmnummer on 6/3/17.
 */
public class Main {

    public static void main(String[] args){
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        System.out.println("-----------------------------");

        IMap map = hz1.getMap("foo");
        for(int k=0;k<100;k++){
            map.put(k,k);
        }
        System.out.println("Done putting ===================");
    }
}
