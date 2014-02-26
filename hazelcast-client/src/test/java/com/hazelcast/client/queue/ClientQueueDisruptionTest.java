package com.hazelcast.client.queue;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.modularhelpers.SimpleClusterUtil;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Queue;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientQueueDisruptionTest {

    HazelcastInstance client1;
    HazelcastInstance client2;

    SimpleClusterUtil cluster;

    @Before
    public void init(){
        Hazelcast.shutdownAll();

        cluster = new SimpleClusterUtil(3);
        cluster.initCluster();

        ClientConfig clientConfig = new ClientConfig();
        client1  = HazelcastClient.newHazelcastClient(clientConfig);
        client2  = HazelcastClient.newHazelcastClient(clientConfig);
    }

    @Test
    public void clientsConsume_withNodeTerminate() throws InterruptedException {

        final int inital=2000, max = 8000;

        for(int i=0; i<inital; i++){
            cluster.getRandomNode().getQueue("Q1").offer(i);
            cluster.getRandomNode().getQueue("Q2").offer(i);
        }

        int expect=0;
        for(int i=inital; i<max; i++){

            if(i==max/2){
                cluster.terminateRandomNode();
            }

            assertTrue(cluster.getRandomNode().getQueue("Q1").offer(i));
            assertTrue(cluster.getRandomNode().getQueue("Q2").offer(i));

            TestCase.assertEquals( expect, client1.getQueue("Q1").poll() );
            TestCase.assertEquals( expect, client2.getQueue("Q2").poll() );

            expect++;
        }

        for(int i=expect; i<max; i++){

            TestCase.assertEquals( i, client1.getQueue("Q1").poll() );
            TestCase.assertEquals( i, client2.getQueue("Q2").poll() );
        }
    }
}
