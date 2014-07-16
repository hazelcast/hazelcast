package com.hazelcast.client.queue;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.modularhelpers.SimpleClusterUtil;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientQueueDisruptionTest {

    private static final int INITIAL_OFFER_COUNT = 2000;
    private static final int MAX_OFFER_COUNT = 8000;

    private SimpleClusterUtil cluster;

    private HazelcastInstance client1;
    private HazelcastInstance client2;

    @Before
    public void setup() {
        cluster = new SimpleClusterUtil("A", 3);
        cluster.initCluster();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(cluster.getName()));
        client1 = HazelcastClient.newHazelcastClient(clientConfig);
        client2 = HazelcastClient.newHazelcastClient(clientConfig);
    }

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void clientsConsume_withNodeTerminate() throws InterruptedException {
        for (int i = 0; i < INITIAL_OFFER_COUNT; i++) {
            cluster.getRandomNode().getQueue("Q1").offer(i);
            cluster.getRandomNode().getQueue("Q2").offer(i);
        }

        int expect = 0;
        for (int i = INITIAL_OFFER_COUNT; i < MAX_OFFER_COUNT; i++) {

            if (i == MAX_OFFER_COUNT / 2) {
                cluster.terminateRandomNode();
            }

            assertTrue(cluster.getRandomNode().getQueue("Q1").offer(i));
            assertTrue(cluster.getRandomNode().getQueue("Q2").offer(i));

            TestCase.assertEquals(expect, client1.getQueue("Q1").poll());
            TestCase.assertEquals(expect, client2.getQueue("Q2").poll());

            expect++;
        }

        for (int i = expect; i < MAX_OFFER_COUNT; i++) {
            TestCase.assertEquals(i, client1.getQueue("Q1").poll());
            TestCase.assertEquals(i, client2.getQueue("Q2").poll());
        }
    }
}
