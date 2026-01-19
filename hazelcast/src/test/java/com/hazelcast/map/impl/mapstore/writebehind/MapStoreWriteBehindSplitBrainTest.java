/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MapStoreWriteBehindSplitBrainTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testMapStoreThreadsAfterSplitBrain() throws InterruptedException {
        String clusterName = generateRandomString(10);
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "10");
        config.setClusterName(clusterName);

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().addMember("127.0.0.1");

        final TrackingMapStore<Integer, Integer> mapStore = new TrackingMapStore<Integer, Integer>();
        final MapStoreConfig mapStoreConfig = new MapStoreConfig();
        final String mapName = randomMapName("default");
        mapStoreConfig
                .setImplementation(mapStore)
                .setWriteDelaySeconds(1)
                .setWriteBatchSize(10)
                .setWriteCoalescing(false);
        config.getMapConfig(mapName)
                .setBackupCount(1)
                .setMapStoreConfig(mapStoreConfig);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSize(3, h1, h3);
        assertClusterSizeEventually(3, h2);

        final CountDownLatch splitLatch = new CountDownLatch(2);
        h3.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }

        });

        final CountDownLatch mergeLatch = new CountDownLatch(1);
        h3.getLifecycleService().addLifecycleListener(new MergedEventLifeCycleListener(mergeLatch));

        final int numberOfItems = 1024;
        h1.getLoggingService().getLogger("mapStore").info("populating h1 " + h1.getName());
        populateMap(h1.getMap(mapName), mapStore, numberOfItems);
        assertTrueEventually("expected all entries to be written", () ->
            assertTrue(mapStore.allExpectedWritesComplete()), 10);

        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);

        assertTrue(splitLatch.await(10, TimeUnit.SECONDS));
        assertClusterSizeEventually(2, h1, h2);
        assertClusterSize(1, h3);

        assertTrue(mergeLatch.await(30, TimeUnit.SECONDS));
        assertClusterSizeEventually(3, h1, h2, h3);

        assertClusterState(ACTIVE, h1, h2, h3);
        h1.getMap(mapName).flush();
        assertTrueEventually("write behinds complete", () ->
            assertTrue(mapStore.lastWriteAtLeastMsAgo(2000)), 30);

        // this will generate thread conflicts on h3
        populateMap(h1.getMap(mapName), mapStore, numberOfItems);
        h1.getMap(mapName).flush();

        assertTrueEventually("expected all entries to be written", () ->
            assertTrue(mapStore.allExpectedWritesComplete()), 30);
    }

    public static class MergedEventLifeCycleListener implements LifecycleListener {
        private final CountDownLatch mergeLatch;

        MergedEventLifeCycleListener(CountDownLatch mergeLatch) {
            this.mergeLatch = mergeLatch;
        }

        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleState.MERGED) {
                mergeLatch.countDown();
            }
        }
    }

    private void populateMap(IMap<Integer, Integer> map,
            TrackingMapStore<Integer, Integer> trackingMapStore,
            int numberOfItems) throws InterruptedException {
        for (int i = 0; i < numberOfItems; i++) {
            trackingMapStore.expectWrite(i, i);
            map.put(i, i);
            Thread.sleep(3); // ensure writes happen across multiple write-behind invocations
        }
    }
}
