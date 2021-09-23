package com.hazelcast.client.queue;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.QueueResetAgeStatisticsCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.LocalQueueStats;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueueResetAgeStatisticsTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastClientInstanceImpl client;
    private HazelcastInstance member;

    @Before
    public void setup() {
        member = hazelcastFactory.newHazelcastInstance();
        client = ((HazelcastClientProxy) hazelcastFactory.newHazelcastClient()).client;
    }

    @Test
    public void testAgeStatsReset()
            throws ExecutionException, InterruptedException {
        IQueue<String> myQueue = client.getQueue("my-queue");
        assertEquals(0, member.getQueue("my-queue").getLocalQueueStats().getMinAge());
        myQueue.add("item-1");
        myQueue.add("item-2");
        Thread.sleep(50);
        myQueue.take();
        myQueue.take();

        LocalQueueStats stats = member.getQueue("my-queue").getLocalQueueStats();
        assertTrue(stats.getMaxAge() > 0);
        assertTrue(stats.getMinAge() > 0);
        assertTrue(stats.getAverageAge() > 0);

        ClientMessage clientMessage = QueueResetAgeStatisticsCodec.encodeRequest("my-queue");
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, "my-queue");
        invocation.invoke().get();
        LocalQueueStats statsAfterReset = member.getQueue("my-queue").getLocalQueueStats();
        assertEquals(0, statsAfterReset.getMaxAge());
        assertEquals(0, statsAfterReset.getMinAge());
        assertEquals(0, statsAfterReset.getAverageAge());

        myQueue.add("item-3");
        myQueue.add("item-4");
        Thread.sleep(20);
        myQueue.take();
        Thread.sleep(100);
        myQueue.take();

        LocalQueueStats populatedStatsAfterReset = member.getQueue("my-queue").getLocalQueueStats();
        assertTrue(statsAfterReset.getMinAge() > 20);
        assertTrue(statsAfterReset.getMaxAge() > 100);
        assertEquals((populatedStatsAfterReset.getMinAge() + populatedStatsAfterReset.getMaxAge()) / 2,
                populatedStatsAfterReset.getAverageAge()
        );


    }

    @After
    public void shutdown() {
        client.shutdown();
        hazelcastFactory.shutdownAll();
    }

}
