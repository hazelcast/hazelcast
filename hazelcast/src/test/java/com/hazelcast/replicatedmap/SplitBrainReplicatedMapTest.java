package com.hazelcast.replicatedmap;


import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.WatchedOperationExecutor;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class SplitBrainReplicatedMapTest {

    @Test
    public void play1() throws Exception {

        HzClusterUtil a = new HzClusterUtil("A", 25701, 4);
        a.initCluster();

        final ReplicatedMap<Object, Object> map = a.getNode(0).getReplicatedMap("default");

        for(int i=0; i<100; i++){
            map.put(i, i);
        }

        final ReplicatedMap<Object, Object> mapx = a.getNode(3).getReplicatedMap("default");


        HzClusterUtil.assertTrueEventually(
            new AssertTask() {
                public void run() {
                    assertEquals(100, map.size());
                    assertEquals(100, mapx.size());
                }
            }
        );

        HzClusterUtil b = a.splitCluster();

        a.assertClusterSizeEventually(2);
        b.assertClusterSizeEventually(2);


        final ReplicatedMap<Object, Object> map1 = a.getNode(0).getReplicatedMap("default");

        final ReplicatedMap<Object, Object> map2 = b.getNode(0).getReplicatedMap("default");


        HzClusterUtil.assertTrueEventually(
                new AssertTask() {
                    public void run() {
                        assertEquals(100, map1.size());
                        assertEquals(100, map2.size());
                    }
                }
        );


        a.mergeCluster(b);
        a.assertClusterSizeEventually(4);

        final ReplicatedMap<Object, Object> map3 = a.getNode(0).getReplicatedMap("default");
        HzClusterUtil.assertTrueEventually(
                new AssertTask() {
                    public void run() {
                        assertEquals(100, map3.size());
                    }
                }
        );

    }

}
