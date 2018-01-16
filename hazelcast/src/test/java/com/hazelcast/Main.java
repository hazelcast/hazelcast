package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.spi.properties.GroupProperty;

public class Main {

    public static void main(String[] args){
        Config config = new Config();
        config.setProperty(Diagnostics.ENABLED.getName(),"true");
        config.setProperty(Diagnostics.METRICS_LEVEL.getName(),"debug");
        config.setProperty(GroupProperty.IO_THREAD_COUNT.getName(),"1");
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);


        for(int k=0;k<10;k++){
            hz1.getMap("f").put(k,k);
        }
        System.out.println("=======================================================================================");
        System.out.println(hz1.getMap("f").size());
        System.out.println(hz2.getMap("f").size());
        System.out.println("=======================================================================================");

        System.exit(0);
    }
}
