package com.hazelcast.tpc;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.noop.Noop;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class SanityTest {

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
        Noop noop = node1.getProxy(Noop.class, "foo");

        for (int k = 0; k < 10000; k++) {
            noop.noop(0);
        }
    }
}
