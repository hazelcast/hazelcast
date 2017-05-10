/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.util.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class BackedupMapDataLossTest {

    @Before
    @After
    public  void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testSplitBrain() throws InterruptedException {
        Config config = new Config();
        config.getGroupConfig().setName("split");
        config.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "5");

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("ClusterMapConfig");
        mapConfig.setBackupCount(2);
        config.addMapConfig(mapConfig);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        final CountDownLatch latch = new CountDownLatch(1);
        h3.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleState.MERGED) {
                    latch.countDown();
                }
            }
        });

        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());

        IMap<String, String> map = h1.getMap("ClusterMap");

        map.put("OneKey", "OneValue");
        map.put("TwoKey", "TwoValue");
        map.put("ThreeKey", "ThreeValue");

        assertEquals(3, map.size());

        System.out.println("Put three values in map. Now waiting for 5 secs...");
        Thread.sleep(5000);
        System.out.println();
        System.out.println("1 -> " + map.get("OneKey"));
        System.out.println("2 -> " + map.get("TwoKey"));
        System.out.println("3 -> " + map.get("ThreeKey"));

        System.out.println("All values present. Backup complete. Simulating drop connections now....");

        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);
        closeConnectionBetween(h2, h1);

        Thread.sleep(1000);

        assertEquals(1, h1.getCluster().getMembers().size());

        //assertEquals(1, h2.getCluster().getMembers().size());
        //assertEquals(1, h3.getCluster().getMembers().size());

        HazelcastInstance instances[] = new HazelcastInstance[]{h1, h2, h3};

        System.out.println("Printing resluts of get calls now...");
        for(int i=0; i<instances.length; i++) {
            HazelcastInstance hz = instances[i];
            System.out.println("Hazelcast Instance: "+ hz.getName());
            IMap m = hz.getMap("ClusterMap");

            assertEquals("OneValue", m.get("OneKey"));
            assertEquals("TwoValue", m.get("TwoKey"));
            assertEquals("ThreeValue", m.get("ThreeKey"));

            System.out.println("1 -> "+m.get("OneKey"));
            System.out.println("2 -> "+m.get("TwoKey"));
            System.out.println("3 -> "+m.get("ThreeKey"));
        }

    }

    private void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
       if (h1 == null || h2 == null) return;
       final Node n1 = TestUtil.getNode(h1);
       final Node n2 = TestUtil.getNode(h2);
       n1.clusterService.removeAddress(n2.address);
       n2.clusterService.removeAddress(n1.address);
    }
}

