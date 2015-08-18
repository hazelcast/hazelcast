package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * Created by alarmnummer on 8/17/15.
 */
public class Main {

    public static void main(String[] args){
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        System.out.println("starting");
        IMap map = hz.getMap("foo");
        for(int k=0;k<10000;k++){
            map.put(k,k);
            System.out.println(k);
        }
    }
}
