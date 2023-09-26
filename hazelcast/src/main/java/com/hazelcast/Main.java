package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMap;

public class Main {

    public static void main(String[] args){
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz3 = Hazelcast.newHazelcastInstance(config);

        CPMap map = hz1.getCPSubsystem().getCPMap("foo");
        System.out.println("=========Starting==================");
        map.set("A","1");
        System.out.println(map.get("A"));
        System.out.println("=========Done==================");
    }
}
