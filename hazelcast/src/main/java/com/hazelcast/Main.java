package com.hazelcast;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.io.Serializable;

public class Main {

    public static void main(String[] args){
        HazelcastInstance node = Hazelcast.newHazelcastInstance();
        IMap map = node.getMap("default");

        final int count =  1000;
        for (int i = 0; i < count; i++) {
            map.put(i, new Foo());

            if(i%1000==0){
                System.out.println("at:"+i);
            }
        }
        System.out.println("Finished inserting");

        System.out.println(map.aggregate(Aggregators.count()));
    }

    static class Foo implements Serializable{
    }
}
