package com.hazelcast.client.io;


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
public class ClusterFluxTest {

    public MySetup test;

    class MySetup extends TestScenarios {

        public ActionBlocks stableNode = new ActionBlocks("1", "n");

        public ActionBlocks client = new ActionBlocks("1", "c");

        public void init(){
            Hazelcast.shutdownAll();

            cluster = new SimpleClusterUtil("A", 4);

            Config cnf = cluster.getConfig();
            MapConfig mapCnf=cnf.getMapConfig("Z");
            mapCnf.setTimeToLiveSeconds(900);
            NearCacheConfig nearCacheConfig=new NearCacheConfig();
            nearCacheConfig.setTimeToLiveSeconds(900);
            mapCnf.setNearCacheConfig(nearCacheConfig);

            cluster.setMinClusterSize(2);
            cluster.setMaxClusterSize(5);
            cluster.initCluster();

            HazelcastInstanceFactory factory = new HazelcastInstanceFactory();
            HazelcastInstance node = factory.newHazelcastInstance(cluster.getConfig());
            stableNode.setHzInstance(node);

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.setGroupConfig(new GroupConfig(cluster.getName()));
            HazelcastInstance clientInstance = HazelcastClient.newHazelcastClient(clientConfig);

            client.setHzInstance(clientInstance);
        }
    }

    public ClusterFluxTest(){
        test = new MySetup();
    }

    @Before
    public void initTest(){
        test.init();
    }


    @Test
    public void lockGetFromStableNode_WhileClusterFlux(){
        test.clusterFlux.run(test.stableNode.lockGet);
        assertEquals(test.clusterFlux.maxIterations, test.stableNode.lockGet.lock.getLockCount());
    }

    @Test  //looks like the cluster is doing a good job of know the key owner while the cluster is changing.
    public void executorCallsFromStableNode_WhileClusterFlux(){
        test.clusterFlux.run(test.stableNode.executeOnKeyOwner);
        assertEquals(test.clusterFlux.maxIterations, test.stableNode.executeOnKeyOwner.totalCalls);
    }



    @Test
    public void mapPutFromStableNode_WhileTerminatingClusterNodes(){
        test.terminate3.run(test.stableNode.mapPut);
        assertEquals(test.terminate3.maxIterations, test.stableNode.mapPut.map.size());
    }

    @Test
    @Category(ProblematicTest.class)
    public void listAddFromStableNode_WhileTerminatingClusterNodes(){
        test.terminate3.run(test.stableNode.listAdd);
        assertEquals(test.terminate3.maxIterations, test.stableNode.listAdd.list.size());
    }

    @Test
    public void setAddFromStableNode_WhileTerminatingClusterNodes(){
        test.terminate3.run(test.stableNode.setAdd);
        assertEquals(test.terminate3.maxIterations, test.stableNode.setAdd.set.size());
    }

    @Test
    public void queueOfferFromStableNode_WhileTerminatingClusterNodes(){
        test.terminate3.run(test.stableNode.queueOffer);
        assertEquals(test.terminate3.maxIterations, test.stableNode.queueOffer.queue.size());
    }

    @Test
    public void lockGetFromStableNode_WhileTerminatingClusterNodes(){
        test.terminate3.run(test.stableNode.lockGet);
        assertEquals(test.terminate3.maxIterations, test.stableNode.lockGet.lock.getLockCount());
    }

    @Test
    public void executorCallsFromStableNode_WhileTerminatingClusterNodes(){
        test.terminate3.run(test.stableNode.executeOnKeyOwner);
        assertEquals(test.terminate3.maxIterations, test.stableNode.executeOnKeyOwner.totalCalls);
    }



    @Test @Category(ProblematicTest.class)
    public void mapPutGetFromStableNode_WhileClusterFlux(){
        test.clusterFlux.run(test.stableNode.mapPut, test.stableNode.mapGet);
        assertEquals(test.clusterFlux.maxIterations, test.stableNode.mapPut.map.size());
    }

    @Test
    @Category(ProblematicTest.class)
    public void mapPutFromStableNode_withAsyncGet_WhileClusterFlux(){
        test.clusterFlux.run(test.stableNode.mapPut, test.stableNode.mapGetAsync);
        assertEquals(test.clusterFlux.maxIterations, test.stableNode.mapPut.map.size());
    }

    @Test
    @Category(ProblematicTest.class)
    public void mapAsyncPutFromStableNode_withAsyncGet_WhileClusterFlux(){
        test.clusterFlux.run(test.stableNode.mapPutAsync, test.stableNode.mapGetAsync);
        assertEquals(test.clusterFlux.maxIterations, test.stableNode.mapPut.map.size());
    }

    @Test
    @Category(ProblematicTest.class)
    public void mapAsyncPutFromStableNode_withGet_WhileClusterFlux(){
        test.clusterFlux.run(test.stableNode.mapPutAsync, test.stableNode.mapGet);
        assertEquals(test.clusterFlux.maxIterations, test.stableNode.mapPut.map.size());
    }



    @Test @Category(ProblematicTest.class)
    public void listAddFromStableNode_WhileClusterFlux(){
        test.clusterFlux.run(test.stableNode.listAdd);
        assertEquals(test.clusterFlux.maxIterations, test.stableNode.listAdd.list.size());
    }

    @Test @Category(ProblematicTest.class)
    public void setAddFromStableNode_WhileClusterFlux(){
        test.clusterFlux.run(test.stableNode.setAdd);
        assertEquals(test.clusterFlux.maxIterations, test.stableNode.setAdd.set.size());
    }

    @Test @Category(ProblematicTest.class)
    public void queueOfferFromStableNode_WhileClusterFlux(){
        test.clusterFlux.run(test.stableNode.queueOffer);
        assertEquals(test.clusterFlux.maxIterations, test.stableNode.queueOffer.queue.size());
    }


    @Test
    @Category(ProblematicTest.class)
    //random fail with
    //java.lang.NullPointerException at com.hazelcast.client.ClientEngineImpl.connectionRemoved(ClientEngineImpl.java:238)
    //com.hazelcast.spi.exception.TargetDisconnectedException:
    public void mapPutFromClient_WhileTerminatingClusterNodes(){
        test.terminate3.run(test.client.mapPut);
        assertEquals(test.terminate3.maxIterations, test.client.mapPut.map.size());
    }

    @Test
    @Category(ProblematicTest.class)
    public void mapPutFromClient_WhileTerminatingClusterNodes_MapFilledFromStableNode(){

        test.happy.run(test.stableNode.mapPut);
        assertEquals(test.happy.maxIterations, test.stableNode.mapPut.map.size());

        test.terminate3.run(test.client.mapPut);
        assertEquals(test.happy.maxIterations + test.terminate3.maxIterations, test.client.mapPut.map.size());
    }



}
