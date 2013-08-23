package com.hazelcast.jmx;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;

public class Main {

    public static void main(String[] args){
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_ENABLE_JMX,"true");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        instance.getMap("foo").put("1","1");
       Hazelcast.newHazelcastInstance(config);
       Hazelcast.newHazelcastInstance(config);

    }
}
