package com.hazelcast.collection.impl.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class QueueMigrationTest extends HazelcastTestSupport {

    private IQueue<Object> queue;
    private HazelcastInstance local;
    private HazelcastInstance remote1;
    private HazelcastInstance remote2;

    @Before
    public void setup() {
        HazelcastInstance[] cluster = createHazelcastInstanceFactory(3).newInstances();
        local = cluster[0];
        remote1 = cluster[1];
        remote2 = cluster[2];

        String name = randomNameOwnedBy(remote1);
        queue = local.getQueue(name);
    }

    @Test
    public void test() {
        List<Object> expectedItems = new LinkedList<Object>();
        for (int k = 0; k < 100; k++) {
            queue.add(k);
            expectedItems.add(k);
        }

        remote1.shutdown();
        remote2.shutdown();

        assertEquals(expectedItems.size(), queue.size());
        List actualItems = Arrays.asList(queue.toArray());
        assertEquals(expectedItems, actualItems);
    }
}
