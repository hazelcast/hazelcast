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
    public void clientsConsume_withNodeTerminate() throws InterruptedException {

        final int inital = 2000, max = 8000;

        for (int i = 0; i < inital; i++) {
            cluster.getRandomNode().getQueue("Q1").offer(i);
            cluster.getRandomNode().getQueue("Q2").offer(i);
        }

        int expect = 0;
        for (int i = inital; i < max; i++) {

            if (i == max / 2) {
                cluster.terminateRandomNode();
            }

            assertTrue(cluster.getRandomNode().getQueue("Q1").offer(i));
            assertTrue(cluster.getRandomNode().getQueue("Q2").offer(i));

            TestCase.assertEquals(expect, client1.getQueue("Q1").poll());
            TestCase.assertEquals(expect, client2.getQueue("Q2").poll());

            expect++;
        }

        for (int i = expect; i < max; i++) {
            TestCase.assertEquals(i, client1.getQueue("Q1").poll());
            TestCase.assertEquals(i, client2.getQueue("Q2").poll());
        }
    }
}
