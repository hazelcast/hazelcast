package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;

/**
 * Created by alarmnummer on 5/13/16.
 */
public class Main extends HazelcastTestSupport {

    public static void main(String[] args) {
        setLoggingLog4j();
        HazelcastInstance hz1= Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2= Hazelcast.newHazelcastInstance();

        IAtomicLong l = hz1.getAtomicLong("x");
        l.set(1);
    }
}
