package com.hazelcast.spi;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class ClusterTest extends HazelcastTestSupport {

    @Test
    public void test() throws InterruptedException {
        HazelcastInstance[] instances = this.createHazelcastInstanceFactory(2).newInstances();

        System.out.println("Waiting");
        Thread.sleep(10000);
        System.out.println("Completed waiting");
    }
}
