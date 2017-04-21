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
import com.hazelcast.instance.Node;
import com.hazelcast.nio.tcp.FirewallingConnectionManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.generateRandomString;
import static com.hazelcast.test.HazelcastTestSupport.getNode;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PartitionedCluster {

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

    public PartitionedCluster partitionFiveMembersThreeAndTwo(MapConfig mapConfig, QuorumConfig quorumConfig) throws Exception {
        createFiveMemberCluster(mapConfig, quorumConfig);
        return splitFiveMembersThreeAndTwo();
    }

    public PartitionedCluster partitionFiveMembersThreeAndTwo(CacheSimpleConfig cacheSimpleConfig, QuorumConfig quorumConfig)
            throws Exception {
        createFiveMemberCluster(cacheSimpleConfig, quorumConfig);
        return splitFiveMembersThreeAndTwo();
    }

    public PartitionedCluster partitionFiveMembersThreeAndTwo(QueueConfig qConfig, QuorumConfig quorumConfig) throws Exception {
        createFiveMemberCluster(qConfig, quorumConfig);
        return splitFiveMembersThreeAndTwo();
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

    public PartitionedCluster splitFiveMembersThreeAndTwo() throws Exception {
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

        assertTrue(splitLatch.await(30, TimeUnit.SECONDS));
        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);
        assertClusterSizeEventually(2, h4);
        assertClusterSizeEventually(2, h5);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertFalse(h4.getQuorumService().getQuorum(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME).isPresent());
                assertFalse(h5.getQuorumService().getQuorum(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME).isPresent());
            }
        });
        return this;
    }

    private void createInstances(Config config) {
        h1 = factory.newHazelcastInstance(config);
        h2 = factory.newHazelcastInstance(config);
        h3 = factory.newHazelcastInstance(config);
        h4 = factory.newHazelcastInstance(config);
        h5 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(5, h1);
        assertClusterSizeEventually(5, h2);
        assertClusterSizeEventually(5, h3);
        assertClusterSizeEventually(5, h4);
        assertClusterSizeEventually(5, h5);
    }

    private QuorumConfig createSuccessfulSplitTestQuorum() {
        QuorumConfig splitConfig = new QuorumConfig();
        splitConfig.setEnabled(true);
        splitConfig.setSize(3);
        splitConfig.setName(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME);
        return splitConfig;
    }

    private void splitCluster() {
        Node n1 = getNode(h1);
        Node n2 = getNode(h2);
        Node n3 = getNode(h3);
        Node n4 = getNode(h4);
        Node n5 = getNode(h5);

        FirewallingConnectionManager cm1 = getConnectionManager(n1);
        FirewallingConnectionManager cm2 = getConnectionManager(n2);
        FirewallingConnectionManager cm3 = getConnectionManager(n3);
        FirewallingConnectionManager cm4 = getConnectionManager(n4);
        FirewallingConnectionManager cm5 = getConnectionManager(n5);

        cm1.block(n4.address);
        cm2.block(n4.address);
        cm3.block(n4.address);

        cm1.block(n5.address);
        cm2.block(n5.address);
        cm3.block(n5.address);

        cm4.block(n1.address);
        cm4.block(n2.address);
        cm4.block(n3.address);

        cm5.block(n1.address);
        cm5.block(n2.address);
        cm5.block(n3.address);

        n4.clusterService.suspectMember(n1.getLocalMember(), null, true);
        n4.clusterService.suspectMember(n2.getLocalMember(), null, true);
        n4.clusterService.suspectMember(n3.getLocalMember(), null, true);

        n5.clusterService.suspectMember(n1.getLocalMember(), null, true);
        n5.clusterService.suspectMember(n2.getLocalMember(), null, true);
        n5.clusterService.suspectMember(n3.getLocalMember(), null, true);

        n1.clusterService.suspectMember(n4.getLocalMember(), null, true);
        n2.clusterService.suspectMember(n4.getLocalMember(), null, true);
        n3.clusterService.suspectMember(n4.getLocalMember(), null, true);

        n1.clusterService.suspectMember(n5.getLocalMember(), null, true);
        n2.clusterService.suspectMember(n5.getLocalMember(), null, true);
        n3.clusterService.suspectMember(n5.getLocalMember(), null, true);
    }

    private static FirewallingConnectionManager getConnectionManager(Node node) {
        return (FirewallingConnectionManager) node.getConnectionManager();
    }
}
