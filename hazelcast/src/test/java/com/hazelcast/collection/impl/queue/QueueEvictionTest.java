package com.hazelcast.collection.impl.queue;

import com.hazelcast.config.Config;
import com.hazelcast.core.DistributedObjectEvent;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
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
@Category({QuickTest.class, ParallelTest.class})
public class QueueEvictionTest extends HazelcastTestSupport {

    @Test
    public void testQueueEviction_whenTtlIsSet_thenTakeThrowsException() throws Exception {
        String queueName = randomString();

        Config config = new Config();
        config.getQueueConfig(queueName).setEmptyQueueTtl(2);
        HazelcastInstance hz = createHazelcastInstance(config);
        IQueue<Object> queue = hz.getQueue(queueName);

        try {
            assertTrue(queue.offer("item"));
            assertEquals("item", queue.poll());

            queue.take();
            fail();
        } catch (DistributedObjectDestroyedException expected) {
            ignore(expected);
        }
        assertEquals(0, queue.size());
    }

    @Test
    public void testQueueEviction_whenTtlIsZero_thenListenersAreNeverthelessExecuted() throws Exception {
        String queueName = randomString();

        Config config = new Config();
        config.getQueueConfig(queueName).setEmptyQueueTtl(0);
        HazelcastInstance hz = createHazelcastInstance(config);

        final CountDownLatch latch = new CountDownLatch(2);
        hz.addDistributedObjectListener(new DistributedObjectListener() {
            public void distributedObjectCreated(DistributedObjectEvent event) {
                latch.countDown();
            }

            public void distributedObjectDestroyed(DistributedObjectEvent event) {
                latch.countDown();
            }
        });

        IQueue<Object> queue = hz.getQueue(queueName);
        assertTrue(queue.offer("item"));
        assertEquals("item", queue.poll());

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }
}
