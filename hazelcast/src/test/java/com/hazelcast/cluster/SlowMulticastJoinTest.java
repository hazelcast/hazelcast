/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.SplitBrainHandlerTest.MergedEventLifeCycleListener;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.cluster.impl.MulticastJoiner;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class SlowMulticastJoinTest extends AbstractJoinTest {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testMembersStaysIndependentWhenHostIsNotTrusted() {
        Config config1 = newConfig("8.8.8.8"); //8.8.8.8 is never a local address
        Config config2 = newConfig("8.8.8.8");

        int testDurationSeconds = 30;
        assertIndependentClustersAndDoNotMergedEventually(config1, config2, testDurationSeconds);
    }

    @Test
    public void testMembersFormAClusterWhenHostIsTrusted() throws Exception {
        Config config2 = newConfig("*.*.*.*"); //matching everything

        testJoin(config2);
    }

    @Test
    public void testSplitBrainMessagesNotAccumulated_whenClusterIsStableOrNodeIsNotMaster() throws Exception {
        final int clusterSize = 3;
        Config config = new Config();
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "15");

        final HazelcastInstance[] instances = new HazelcastInstance[clusterSize];
        final MulticastJoiner[] joiners = new MulticastJoiner[clusterSize];

        for (int i = 0; i < clusterSize; i++) {
            instances[i] = Hazelcast.newHazelcastInstance(config);
            joiners[i] = (MulticastJoiner) getNode(instances[i]).getJoiner();
        }

        assertClusterSize(clusterSize, instances[0]);

        // we will split the cluster to subclusters (0, 1), (2)
        final CountDownLatch splitLatch = new CountDownLatch(2);
        instances[2].getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }

        });

        final CountDownLatch mergeLatch = new CountDownLatch(1);
        instances[2].getLifecycleService().addLifecycleListener(new MergedEventLifeCycleListener(mergeLatch));

        // while cluster is stable, split brain join messages should not be accumulated.
        // the master member for the 10 seconds duration of assertion may have up to as many as other members
        // in the cluster (clusterSize-1) messages in its queue which will be ignored by the SplitBrainHandler.
        assertSplitBrainMessagesCount(clusterSize, instances, joiners);

        // split cluster
        closeConnectionBetween(instances[0], instances[2]);
        closeConnectionBetween(instances[1], instances[2]);

        assertTrue(splitLatch.await(10, TimeUnit.SECONDS));

        // while cluster is split, no split brain join messages should be accumulated in the non-master member 1
        assertSplitBrainMessagesCount(clusterSize, new HazelcastInstance[]{instances[1]},
                new MulticastJoiner[]{joiners[1]});

        assertTrue(mergeLatch.await(30, TimeUnit.SECONDS));
        assertClusterSize(clusterSize, instances[0]);

        // cluster is merged & stable again, split brain join messages should not be accumulated.
        assertSplitBrainMessagesCount(clusterSize, instances, joiners);
    }

    private void assertSplitBrainMessagesCount(final int clusterSize, final HazelcastInstance[] instances,
                                               final MulticastJoiner[] joiners) {
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (int i = 0; i < instances.length; i++) {
                    // a master can have at most (clusterSize-1) split brain join messages
                    if (getNode(instances[i]).isMaster()) {
                        assertTrue(joiners[i].getSplitBrainMessagesCount() < clusterSize);
                    } else {
                        // other members should not have any split brain join messages
                        assertEquals(0, joiners[i].getSplitBrainMessagesCount());
                    }
                }
            }
        }, 10);
    }

    private Config newConfig(String trustedInterface) {
        Config config = new Config();
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        config.getNetworkConfig().getJoin().getMulticastConfig().addTrustedInterface(trustedInterface);
        return config;
    }
}
