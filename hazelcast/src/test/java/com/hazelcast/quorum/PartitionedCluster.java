/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.closeConnectionBetween;
import static com.hazelcast.test.HazelcastTestSupport.generateRandomString;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PartitionedCluster {

    public static final String QUORUM_ID = "threeNodeQuorumRule";

    private static final String SUCCESSFUL_SPLIT_TEST_QUORUM_NAME = "SUCCESSFUL_SPLIT_TEST_QUORUM";

    public HazelcastInstance h1;
    public HazelcastInstance h2;
    public HazelcastInstance h3;
    public HazelcastInstance h4;
    public HazelcastInstance h5;

    protected TestHazelcastInstanceFactory factory;

    public PartitionedCluster(TestHazelcastInstanceFactory factory) {
        this.factory = factory;
    }

    public PartitionedCluster partitionFiveMembersThreeAndTwo(MapConfig mapConfig, QuorumConfig quorumConfig) {
        createFiveMemberCluster(mapConfig, quorumConfig);
        return splitFiveMembersThreeAndTwo(quorumConfig.getName());
    }

    public PartitionedCluster partitionFiveMembersThreeAndTwo(CacheSimpleConfig cacheSimpleConfig, QuorumConfig quorumConfig) {
        createFiveMemberCluster(cacheSimpleConfig, quorumConfig);
        return splitFiveMembersThreeAndTwo(quorumConfig.getName());
    }

    public PartitionedCluster partitionFiveMembersThreeAndTwo(QueueConfig qConfig, QuorumConfig quorumConfig) {
        createFiveMemberCluster(qConfig, quorumConfig);
        return splitFiveMembersThreeAndTwo(quorumConfig.getName());
    }

    private PartitionedCluster createFiveMemberCluster(MapConfig mapConfig, QuorumConfig quorumConfig) {
        Config config = createClusterConfig()
                .addMapConfig(mapConfig)
                .addQuorumConfig(quorumConfig);
        createInstances(config);
        return this;
    }

    public PartitionedCluster createFiveMemberCluster(CacheSimpleConfig cacheSimpleConfig, QuorumConfig quorumConfig) {
        Config config = createClusterConfig()
                .addCacheConfig(cacheSimpleConfig)
                .addQuorumConfig(quorumConfig);
        createInstances(config);
        return this;
    }

    public PartitionedCluster createFiveMemberCluster(QueueConfig queueConfig, QuorumConfig quorumConfig) {
        Config config = createClusterConfig()
                .addQueueConfig(queueConfig)
                .addQuorumConfig(quorumConfig);
        createInstances(config);
        return this;
    }

    public PartitionedCluster createFiveMemberCluster(LockConfig lockConfig, QuorumConfig quorumConfig) {
        Config config = createClusterConfig()
                .addLockConfig(lockConfig)
                .addQuorumConfig(quorumConfig);
        createInstances(config);
        return this;
    }

    private Config createClusterConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "9999");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "9999");
        config.getGroupConfig().setName(generateRandomString(10));
        config.addQuorumConfig(createSuccessfulSplitTestQuorum());
        return config;
    }

    public PartitionedCluster splitFiveMembersThreeAndTwo(String quorumId) {
        final CountDownLatch splitLatch = new CountDownLatch(6);
        h4.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }
        });
        h5.getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }
        });

        splitCluster();

        assertOpenEventually(splitLatch, 30);
        assertClusterSizeEventually(3, h1, h2, h3);
        assertClusterSizeEventually(2, h4, h5);

        verifyQuorums(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME);
        verifyQuorums(quorumId);

        return this;
    }

    private void createInstances(Config config) {
        h1 = factory.newHazelcastInstance(config);
        h2 = factory.newHazelcastInstance(config);
        h3 = factory.newHazelcastInstance(config);
        h4 = factory.newHazelcastInstance(config);
        h5 = factory.newHazelcastInstance(config);

        assertClusterSize(5, h1, h5);
        assertClusterSizeEventually(5, h2, h3, h4);
    }

    private QuorumConfig createSuccessfulSplitTestQuorum() {
        QuorumConfig splitConfig = new QuorumConfig();
        splitConfig.setEnabled(true);
        splitConfig.setSize(3);
        splitConfig.setName(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME);
        return splitConfig;
    }

    private void splitCluster() {
        blockCommunicationBetween(h1, h4);
        blockCommunicationBetween(h1, h5);

        blockCommunicationBetween(h2, h4);
        blockCommunicationBetween(h2, h5);

        blockCommunicationBetween(h3, h4);
        blockCommunicationBetween(h3, h5);

        closeConnectionBetween(h4, h3);
        closeConnectionBetween(h4, h2);
        closeConnectionBetween(h4, h1);

        closeConnectionBetween(h5, h3);
        closeConnectionBetween(h5, h2);
        closeConnectionBetween(h5, h1);
    }

    private void verifyQuorums(String quorumId) {
        assertQuorumIsPresentEventually(h1, quorumId);
        assertQuorumIsPresentEventually(h2, quorumId);
        assertQuorumIsPresentEventually(h3, quorumId);
        assertQuorumIsAbsentEventually(h4, quorumId);
        assertQuorumIsAbsentEventually(h5, quorumId);
    }

    private void assertQuorumIsPresentEventually(final HazelcastInstance instance, final String quorumId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(instance.getQuorumService().getQuorum(quorumId).isPresent());
            }
        });
    }

    private void assertQuorumIsAbsentEventually(final HazelcastInstance instance, final String quorumId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse(instance.getQuorumService().getQuorum(quorumId).isPresent());
            }
        });
    }

}
