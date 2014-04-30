package com.hazelcast.replicatedmap;


import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.WatchedOperationExecutor;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.annotation.ProblematicTest;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class SplitBrainReplicatedMapTest {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test
    public void splitClusterReplicatedMap_AssertFromAllNodes_InMemoryFormat_Object() throws Exception {
        splitClusterReplicatedMap_AssertFromAllNodes(InMemoryFormat.OBJECT, 0);
    }

    @Test
    public void splitClusterReplicatedMap_AssertFromAllNodes_InMemoryFormat_Binary() throws Exception {
        splitClusterReplicatedMap_AssertFromAllNodes(InMemoryFormat.BINARY, 0);
    }

    @Test
    public void splitClusterReplicatedMap_AssertFromAllNodes_InMemoryFormat_Object_RepDelay() throws Exception {
        splitClusterReplicatedMap_AssertFromAllNodes(InMemoryFormat.OBJECT, 1000);
    }

    @Test
    public void splitClusterReplicatedMap_AssertFromAllNodes_InMemoryFormat_Binary_RepDelay() throws Exception {
        splitClusterReplicatedMap_AssertFromAllNodes(InMemoryFormat.BINARY, 1000);
    }



    public void splitClusterReplicatedMap_AssertFromAllNodes(InMemoryFormat inMemoryFormat, int delay) throws Exception {

        final HzClusterUtil a = new HzClusterUtil("A", 25701, 4);
        for(Config cfg : a.getConfigs()){
            cfg.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);
            cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(delay);
        }
        a.initCluster();

        for(int i=0; i<100; i++){
            a.getRandomNode().getReplicatedMap("default").put(i,i);
        }

        assertTrueEventually(
                new AssertTask() {
                    public void run() {
                        for (HazelcastInstance hz : a.getCluster())
                            assertEquals(100, hz.getReplicatedMap("default").size());
                    }
                }
        );

        final HzClusterUtil b = a.splitCluster();

        a.assertClusterSizeEventually(2);
        b.assertClusterSizeEventually(2);

        assertTrueEventually(
                new AssertTask() {
                    public void run() {
                        for (HazelcastInstance hz : a.getCluster())
                            assertEquals(100, hz.getReplicatedMap("default").size());

                        for (HazelcastInstance hz : b.getCluster())
                            assertEquals(100, hz.getReplicatedMap("default").size());
                    }
                }
        );
    }


    @Test
    @Category(ProblematicTest.class)
    public void splitJoinedClusterReplicatedMap_AssertFromAllNodes_InMemoryFormat_Object() throws Exception {
        splitJoinedClusterReplicatedMap_AssertFromAllNodes(InMemoryFormat.OBJECT, 0);
    }

    @Test
    @Category(ProblematicTest.class)
    public void splitJoinedClusterReplicatedMap_AssertFromAllNodes_InMemoryFormat_Binary() throws Exception {
        splitJoinedClusterReplicatedMap_AssertFromAllNodes(InMemoryFormat.BINARY, 0);
    }

    @Test
    @Category(ProblematicTest.class)
    public void splitJoinedClusterReplicatedMap_AssertFromAllNodes_InMemoryFormat_Object_RepDealy() throws Exception {
        splitJoinedClusterReplicatedMap_AssertFromAllNodes(InMemoryFormat.OBJECT, 1000);
    }

    @Test
    @Category(ProblematicTest.class)
    public void splitJoinedClusterReplicatedMap_AssertFromAllNodes_InMemoryFormat_Binary_RepDealy() throws Exception {
        splitJoinedClusterReplicatedMap_AssertFromAllNodes(InMemoryFormat.BINARY, 1000);
    }



    public void splitJoinedClusterReplicatedMap_AssertFromAllNodes(InMemoryFormat inMemoryFormat, int delay) throws Exception {

        final HzClusterUtil a = new HzClusterUtil("A", 25701, 4);
        for(Config cfg : a.getConfigs()){
            cfg.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);
            cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(delay);
        }
        a.initCluster();

        for(int i=0; i<100; i++){
            a.getRandomNode().getReplicatedMap("default").put(i,i);
        }

        assertTrueEventually(
                new AssertTask() {
                    public void run() {
                        for (HazelcastInstance hz : a.getCluster())
                            assertEquals(100, hz.getReplicatedMap("default").size());
                    }
                }
        );

        final HzClusterUtil b = a.splitCluster();

        a.assertClusterSizeEventually(2);
        b.assertClusterSizeEventually(2);

        assertTrueEventually(
                new AssertTask() {
                    public void run() {
                        for (HazelcastInstance hz : a.getCluster())
                            assertEquals(100, hz.getReplicatedMap("default").size());

                        for (HazelcastInstance hz : b.getCluster())
                            assertEquals(100, hz.getReplicatedMap("default").size());
                    }
                }
        );


        for(int i=0; i<100; i++){
            a.getRandomNode().getReplicatedMap("default").put(i+"A",i);
            b.getRandomNode().getReplicatedMap("default").put(i+"B",i);
        }


        assertTrueEventually(
                new AssertTask() {
                    public void run() {
                        for (HazelcastInstance hz : a.getCluster())
                            assertEquals(200, hz.getReplicatedMap("default").size());

                        for (HazelcastInstance hz : b.getCluster())
                            assertEquals(200, hz.getReplicatedMap("default").size());
                    }
                }
        );


        a.mergeCluster(b);
        a.assertClusterSizeEventually(4);

        assertTrueEventually(
                new AssertTask() {
                    public void run() {
                        for (HazelcastInstance hz : a.getCluster())
                            assertEquals(300, hz.getReplicatedMap("default").size());
                    }
                }
        );
    }


}
