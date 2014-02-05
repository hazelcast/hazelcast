package com.hazelcast.client.map;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.modularhelpers.ActionBlocks;
import com.hazelcast.test.modularhelpers.SimpleClusterUtil;
import com.hazelcast.test.modularhelpers.TestScenarios;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapNearCashTestWithClusterProblems {

    public MySetup t;

    class MySetup extends TestScenarios {

        //stableNode in not under the controle of SimpleClusterUtil so cannot be terminated ect, but is part of the HZ cluster
        public ActionBlocks stableNode = new ActionBlocks("Z", "stableNode");

        public ActionBlocks clusterNode = new ActionBlocks("Z", "clusterNode");

        public ActionBlocks client = new ActionBlocks("Z", "clientNode");


        public void init(){
            Hazelcast.shutdownAll();

            cluster = new SimpleClusterUtil("A", 1);

            cluster.setMinClusterSize(0);
            cluster.setMaxClusterSize(3);

            Config cnf = cluster.getConfig();

            MapConfig mapCnf = cnf.getMapConfig("Z");
            mapCnf.setTimeToLiveSeconds(900);

            NearCacheConfig nearCacheConfig = new NearCacheConfig();
            nearCacheConfig.setTimeToLiveSeconds(900);

            mapCnf.setNearCacheConfig(nearCacheConfig);

            cluster.initCluster();

            clusterNode.setHzInstance(cluster.getRandomNode());

            HazelcastInstanceFactory factory = new HazelcastInstanceFactory();
            HazelcastInstance node = factory.newHazelcastInstance(cluster.getConfig());
            stableNode.setHzInstance(node);

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setGroupConfig(new GroupConfig(cluster.getName()));
            HazelcastInstance clientInstance = HazelcastClient.newHazelcastClient(clientConfig);

            client.setHzInstance(clientInstance);
        }
    }

    public MapNearCashTestWithClusterProblems(){
        t = new MySetup();
    }

    @Before
    public void initTest(){
        t.init();
    }


    @Test
    public void containsKeysTest_WhileNodeTerminate(){
        t.happy.run(t.stableNode.mapPut);
        t.terminate1.run(t.stableNode.mapContainsKey);
    }

    @Test
    public void putFromClusterNode_containsKeysTest_WhileNodeTerminate(){
        t.happy.run(t.clusterNode.mapPut);
        t.stableNode.setKeyPrefix("clusterNode");
        t.terminate1.run(t.stableNode.mapContainsKey);
    }

    @Test
    public void putFromClient_containsKeysTest_WhileNodeTerminate(){
        t.happy.run(t.client.mapPut);
        t.stableNode.setKeyPrefix("clientNode");
        t.terminate1.run(t.stableNode.mapContainsKey);
    }


    @Test
    public void testMapContainsKey_AfterNodeTerminateRestart_Where_KeysAreInNearCash_ViaMapGet(){
        t.happy.run(t.stableNode.mapPut, t.clusterNode.mapPut);

        t.stableNode.setKeyPrefix("stableNode");
        t.terminate1.run(t.stableNode.mapContainsKey);

        t.getCluster().addNode();

        t.stableNode.setKeyPrefix("clusterNode");
        t.happy.run(t.stableNode.mapGet);
        t.terminate1.run(t.stableNode.mapContainsKey);
    }

    @Test
    @Category(ProblematicTest.class)
    public void testMapContainsKey_AfterNodeTerminateRestart_Where_KeysNotInNearCash() throws InterruptedException {
        t.happy.run(t.stableNode.mapPut, t.clusterNode.mapPut);

        t.stableNode.setKeyPrefix("stableNode");
        t.terminate1.run(t.stableNode.mapContainsKey);

        t.getCluster().addNode();

        System.out.println("SIZE ===>>>"+t.stableNode.mapPut.map.size());  //it is possible we see the correct map size

        t.stableNode.setKeyPrefix("clusterNode");
        t.terminate1.run(t.stableNode.mapContainsKey);  //but we cannot find the key in the map ?
    }

    @Test
    @Category(ProblematicTest.class)
    public void testMapContainsKey_AfterNodeTerminateRestart(){
        t.happy.run(t.stableNode.mapPut, t.clusterNode.mapPut, t.client.mapPut);

        t.stableNode.setKeyPrefix("stableNode");
        t.terminate1.run(t.stableNode.mapContainsKey);

        t.getCluster().addNode();

        System.out.println("SIZE ===>>>"+t.stableNode.mapPut.map.size());//size is ok hear


        t.stableNode.setKeyPrefix("clusterNode");
        t.happy.run(t.stableNode.mapGet);                       //after MapAction(mapGet) IMap{name='Z'} name(Z) ===>>>15000
        t.terminate1.run(t.stableNode.mapContainsKey);          //after MapAction(mapContainsKey) IMap{name='Z'} name(Z) ===>>>14880 ?

        System.out.println("SIZE ===>>>"+t.stableNode.mapPut.map.size());
        t.getCluster().addNode();
        System.out.println("SIZE ===>>>"+t.stableNode.mapPut.map.size());//the size is small hear


        t.stableNode.setKeyPrefix("clientNode");
        t.happy.run(t.stableNode.mapGet);
        t.terminate1.run(t.stableNode.mapContainsKey);
    }
}
