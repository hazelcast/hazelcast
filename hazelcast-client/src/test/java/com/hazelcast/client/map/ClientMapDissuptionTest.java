package com.hazelcast.client.map;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.*;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.modularhelpers.SimpleClusterUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientMapDissuptionTest {

    HazelcastInstance client;

    HazelcastInstance stableNode;
    SimpleClusterUtil cluster;


    @Before
    public void init(){
        Hazelcast.shutdownAll();

        cluster = new SimpleClusterUtil("A", 1);
        cluster.initCluster();

        HazelcastInstanceFactory factory = new HazelcastInstanceFactory();
        stableNode  = factory.newHazelcastInstance(cluster.getConfig());

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(cluster.getName()));
        client  = HazelcastClient.newHazelcastClient(clientConfig);
    }




    @Test
    @Category(ProblematicTest.class)
    //Random Fail @Repeat(20) not seen to pass
    public void testClient_WithDisruption() throws InterruptedException {
        play1(client);
    }

    @Test
    @Category(ProblematicTest.class)
    //Random Fail at assertTrue("map size="+map.size() +" key="+i+" not found", map.containsKey(i));
    public void testNode_WithDisruption() throws InterruptedException {
        play1(stableNode);
    }


    public void play1(HazelcastInstance node) throws InterruptedException {

        IMap map = node.getMap("map");

        for(int i=0; i<5000; i++){
            map.put(i, i);
        }

        for(int i=0; i<5000; i++){
            assertTrue(map.containsKey(i));
            if(i==2500){cluster.terminateRandomNode();}
        }

        cluster.addNode();

        assertEquals(5000, map.size());

        for(int i=0; i<5000; i++){
            assertTrue("map size="+map.size() +" key="+i+" not found", map.containsKey(i));
            if(i==2500){cluster.terminateRandomNode();}
        }

    }

}
