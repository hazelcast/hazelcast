package com.hazelcast.tpc.requestservice;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class CompatibilityLayerTest {

    private HazelcastInstance node1;
    private HazelcastInstance node2;

    @Before
    public void before() {
        node1 = Hazelcast.newHazelcastInstance();
        node2 = Hazelcast.newHazelcastInstance();
    }

    @After
    public void after() {
        if (node1 != null) {
            System.out.println("node1 shutdown started");
            node1.shutdown();
            System.out.println("node1 shutdown completed");
        }

        if (node2 != null) {
            System.out.println("node2 shutdown started");
            node2.shutdown();
            System.out.println("node2 shutdown started");
        }
    }

    @Test
    public void test() {
        IMap map = node1.getMap("foo");
        System.out.println("Map created");

        int count = 1000;
        for (int k = 0; k < count; k++) {
            map.put(k, k);
            System.out.println(k);
        }

        System.out.println("Done");

        assertEquals(count, map.size());
    }
}
