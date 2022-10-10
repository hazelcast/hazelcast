package com.hazelcast.alto.requestservice;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.table.Table;
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
        Table table = node.getTable("foo");

        for (int k = 0; k < 10000; k++) {
            table.noop();
        }
    }
}
