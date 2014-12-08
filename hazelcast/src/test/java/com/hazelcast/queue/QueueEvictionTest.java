package com.hazelcast.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class QueueEvictionTest extends AbstractQueueTest{


    //what are you testing? The intent is not clear
    @Test
    public void testQueueEviction() throws Exception {
        final Config config = new Config();
        config.getQueueConfig("q").setEmptyQueueTtl(2);
        final HazelcastInstance hz = createHazelcastInstance(config);
        final IQueue<Object> q = hz.getQueue("q");

        try {
            assertTrue(q.offer("item"));
            assertEquals("item", q.poll());
            q.take();
            fail();
        } catch (DistributedObjectDestroyedException expected) {

        }
        q.size();

    }

    // TODO: Can you come up with a better name. A name with a number is not very informative.
    @Test
    public void testQueueEviction2() throws Exception {
        final Config config = new Config();
        config.getQueueConfig("q2").setEmptyQueueTtl(0);
        final HazelcastInstance hz = createHazelcastInstance(config);

        final CountDownLatch latch = new CountDownLatch(2);
        hz.addDistributedObjectListener(new DistributedObjectListener() {
            public void distributedObjectCreated(DistributedObjectEvent event) {
                latch.countDown();
            }

            public void distributedObjectDestroyed(DistributedObjectEvent event) {
                latch.countDown();
            }
        });

        final IQueue<Object> q = hz.getQueue("q2");
        q.offer("item");
        q.poll();

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

}