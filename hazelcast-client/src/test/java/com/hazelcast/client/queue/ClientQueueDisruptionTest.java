package com.hazelcast.client.queue;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.modularhelpers.SimpleClusterUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientQueueDisruptionTest extends HazelcastTestSupport {

    HazelcastInstance client1;
    HazelcastInstance client2;

    SimpleClusterUtil cluster;

    @Before
    public void init() {
        cluster = new SimpleClusterUtil("A", 3);
        cluster.initCluster();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(cluster.getName()));
        client1 = HazelcastClient.newHazelcastClient(clientConfig);
        client2 = HazelcastClient.newHazelcastClient(clientConfig);
    }

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void clientsConsume_withNodeShutdown() throws InterruptedException {

        final int initial = 2000, max = 8000;

        for (int i = 0; i < initial; i++) {
            cluster.getRandomNode().getQueue("Q1").offer(i);
            cluster.getRandomNode().getQueue("Q2").offer(i);
        }

        int expectCount = 0;
        for (int i = initial; i < max; i++) {

            if (i == max / 2) {
                cluster.shutdownRandomNode();
            }
            final int index = i;

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertTrue(cluster.getRandomNode().getQueue("Q1").offer(index));
                }
            });

            assertExactlyOneSuccessfulRun(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertTrue(cluster.getRandomNode().getQueue("Q2").offer(index));
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
}
