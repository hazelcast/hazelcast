/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.MembershipAdapter;
import com.hazelcast.cluster.MembershipEvent;
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

    static final String QUORUM_ID = "threeNodeQuorumRule";

    private static final String SUCCESSFUL_SPLIT_TEST_QUORUM_NAME = "SUCCESSFUL_SPLIT_TEST_QUORUM";

    public HazelcastInstance[] instance;

    protected TestHazelcastInstanceFactory factory;

    PartitionedCluster(TestHazelcastInstanceFactory factory) {
        this.factory = factory;
    }

    public static Config createClusterConfig(Config config) {
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "9999");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "9999");
        config.setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10");
        config.setProperty(GroupProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        config.getGroupConfig().setName(generateRandomString(10));
        config.addQuorumConfig(createSuccessfulSplitTestQuorum());
        return config;
    }

    private static QuorumConfig createSuccessfulSplitTestQuorum() {
        QuorumConfig splitConfig = new QuorumConfig();
        splitConfig.setEnabled(true);
        splitConfig.setSize(3);
        splitConfig.setName(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME);
        return splitConfig;
    }

    public HazelcastInstance getInstance(int index) {
        return instance[index];
    }

    public void createFiveMemberCluster(Config config) {
        createInstances(config);
    }

    public void splitFiveMembersThreeAndTwo(String... quorumIds) {
        final CountDownLatch splitLatch = new CountDownLatch(6);
        instance[3].getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }
        });
        instance[4].getCluster().addMembershipListener(new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }
        });

        splitCluster();

        assertOpenEventually(splitLatch, 60);
        assertClusterSizeEventually(3, instance[0], instance[1], instance[2]);
        assertClusterSizeEventually(2, instance[3], instance[4]);

        verifyQuorums(SUCCESSFUL_SPLIT_TEST_QUORUM_NAME);
        for (String quorumId : quorumIds) {
            verifyQuorums(quorumId);
        }
    }

    private void createInstances(Config config) {
        if (instance == null) {
            instance = new HazelcastInstance[5];
            instance = factory.newInstances(config, 5);
        }
        assertClusterSize(5, instance[0], instance[4]);
        assertClusterSizeEventually(5, instance[1], instance[2], instance[3]);
    }

    private void splitCluster() {
        blockCommunicationBetween(instance[0], instance[3]);
        blockCommunicationBetween(instance[0], instance[4]);

        blockCommunicationBetween(instance[1], instance[3]);
        blockCommunicationBetween(instance[1], instance[4]);

        blockCommunicationBetween(instance[2], instance[3]);
        blockCommunicationBetween(instance[2], instance[4]);

        closeConnectionBetween(instance[3], instance[2]);
        closeConnectionBetween(instance[3], instance[1]);
        closeConnectionBetween(instance[3], instance[0]);

        closeConnectionBetween(instance[4], instance[2]);
        closeConnectionBetween(instance[4], instance[1]);
        closeConnectionBetween(instance[4], instance[0]);
    }

    private void verifyQuorums(String quorumId) {
        assertQuorumIsPresentEventually(instance[0], quorumId);
        assertQuorumIsPresentEventually(instance[1], quorumId);
        assertQuorumIsPresentEventually(instance[2], quorumId);
        assertQuorumIsAbsentEventually(instance[3], quorumId);
        assertQuorumIsAbsentEventually(instance[4], quorumId);
    }

    private void assertQuorumIsPresentEventually(final HazelcastInstance instance, final String quorumId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(instance.getQuorumService().getQuorum(quorumId).isPresent());
            }
        });
    }

    private void assertQuorumIsAbsentEventually(final HazelcastInstance instance, final String quorumId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(instance.getQuorumService().getQuorum(quorumId).isPresent());
            }
        });
    }
}
