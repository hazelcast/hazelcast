package com.hazelcast.alto.requestservice;

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
public class CompatibilityModeTest {

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
            node1.shutdown();
        }

        if (node2 != null) {
            node2.shutdown();
        }
    }

    @Test
    public void test() {
        IMap map = node1.getMap("foo");

        int count = 10_000;
        for (int k = 0; k < count; k++) {
            map.put(k, k);
        }

        assertEquals(count, map.size());
    }
}
