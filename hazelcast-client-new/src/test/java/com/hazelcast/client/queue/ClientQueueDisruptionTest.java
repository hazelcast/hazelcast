package com.hazelcast.client.queue;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class ClientQueueDisruptionTest extends HazelcastTestSupport {

    private static final int CLUSTER_SIZE = 3;
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private Random random = new Random();
    private List<HazelcastInstance> cluster;

    private HazelcastInstance client1;
    private HazelcastInstance client2;


    @Before
    public void setup() {
        cluster = new ArrayList<HazelcastInstance>(CLUSTER_SIZE);
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            cluster.add(hazelcastFactory.newHazelcastInstance());
        }

        client1 = HazelcastClient.newHazelcastClient();
        client2 = HazelcastClient.newHazelcastClient();
    }

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Test
    @Ignore
    public void clientsConsume_withNodeShutdown() throws InterruptedException {

        final int initial = 2000, max = 8000;

        for (int i = 0; i < initial; i++) {
            getRandomNode().getQueue("Q1").offer(i);
            getRandomNode().getQueue("Q2").offer(i);
        }

        int expectCount = 0;
        for (int i = initial; i < max; i++) {

            if (i == max / 2) {
                shutdownRandomNode();
            }
            final int index = i;

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertTrue(getRandomNode().getQueue("Q1").offer(index));
                }
            });

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertTrue(getRandomNode().getQueue("Q2").offer(index));
                }
            });

            final int expected = expectCount;

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(expected, client1.getQueue("Q1").poll());
                }
            });

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(expected, client2.getQueue("Q2").poll());
                }
            });


            expectCount++;
        }

        for (int i = expectCount; i < max; i++) {
            assertEquals(i, client1.getQueue("Q1").poll());
            assertEquals(i, client2.getQueue("Q2").poll());
        }
    }

    private HazelcastInstance getRandomNode() {
        int index = random.nextInt(CLUSTER_SIZE);
        return cluster.get(index);
    }

    private void shutdownRandomNode() {
        HazelcastInstance node = getRandomNode();
        node.getLifecycleService().shutdown();
        cluster.remove(node);
    }
}
