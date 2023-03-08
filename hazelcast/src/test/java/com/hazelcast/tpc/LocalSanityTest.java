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
public class LocalSanityTest {

    private HazelcastInstance node;

    @Before
    public void before() {
        node = Hazelcast.newHazelcastInstance();
    }

    @After
    public void after() {
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    public void test() {
        Noop noop = node.getProxy(Noop.class, "foo");

        for (int k = 0; k < 10000; k++) {
            noop.noop(0);
        }
    }
}
