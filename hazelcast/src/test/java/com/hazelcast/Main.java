package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by alarmnummer on 10/22/16.
 */
public class Main extends HazelcastTestSupport{

    @Test
    public void test(){
        setLoggingLog4j();

        System.out.println("====================== hz1 ==========================");
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();

        System.out.println("====================== hz2 ==========================");
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        System.out.println("starting");

        for(int k=0;k<10000;k++){
            hz1.getAtomicLong("k").get();
        }

        System.out.println("done");
    }
}
