package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastTest;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.test.HazelcastTestSupport;

/**
 * Created by alarmnummer on 8/16/15.
 */
public class Main extends HazelcastTestSupport{

    public static void main(String[] args){
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        MetricsRegistry metricsRegistry = getNode(hz).getNodeEngine().getMetricsRegistry();
        for(String name: metricsRegistry.getNames()){
            System.out.println(name);
        }
    }
}
