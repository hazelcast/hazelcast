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

package com.hazelcast.splitbrainprotection;

import com.hazelcast.config.Config;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.MembershipAdapter;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.spi.properties.ClusterProperty;
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

    static final String SPLIT_BRAIN_PROTECTION_ID = "threeNodeSplitBrainProtectionRule";

    private static final String SUCCESSFUL_SPLIT_BRAIN_PROTECTION_TEST_NAME =
            "SUCCESSFUL_SPLIT_BRAIN_PROTECTION_TEST";

    public HazelcastInstance[] instance;

    protected TestHazelcastInstanceFactory factory;

    PartitionedCluster(TestHazelcastInstanceFactory factory) {
        this.factory = factory;
    }

    public static Config createClusterConfig(Config config) {
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "9999");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "9999");
        config.setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        config.setClusterName(generateRandomString(10));
        config.addSplitBrainProtectionConfig(createSuccessfulTestSplitBrainProtection());
        return config;
    }

    private static SplitBrainProtectionConfig createSuccessfulTestSplitBrainProtection() {
        SplitBrainProtectionConfig splitConfig = new SplitBrainProtectionConfig();
        splitConfig.setEnabled(true);
        splitConfig.setMinimumClusterSize(3);
        splitConfig.setName(SUCCESSFUL_SPLIT_BRAIN_PROTECTION_TEST_NAME);
        return splitConfig;
    }

    public HazelcastInstance getInstance(int index) {
        return instance[index];
    }

    public void createFiveMemberCluster(Config config, String[] splitBrainProtectionIds) {
        createInstances(config);
        verifySplitBrainProtectionsPresentEventually(SUCCESSFUL_SPLIT_BRAIN_PROTECTION_TEST_NAME);
        for (String splitBrainProtectionId : splitBrainProtectionIds) {
            verifySplitBrainProtectionsPresentEventually(splitBrainProtectionId);
        }
    }

    public void splitFiveMembersThreeAndTwo(String... splitBrainProtectionIds) {
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

        verifySplitBrainProtections(SUCCESSFUL_SPLIT_BRAIN_PROTECTION_TEST_NAME);
        for (String splitBrainProtectionId : splitBrainProtectionIds) {
            verifySplitBrainProtections(splitBrainProtectionId);
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

    private void verifySplitBrainProtectionsPresentEventually(String splitBrainProtectionId) {
        assertSplitBrainProtectionIsPresentEventually(instance[0], splitBrainProtectionId);
        assertSplitBrainProtectionIsPresentEventually(instance[1], splitBrainProtectionId);
        assertSplitBrainProtectionIsPresentEventually(instance[2], splitBrainProtectionId);
        assertSplitBrainProtectionIsPresentEventually(instance[3], splitBrainProtectionId);
        assertSplitBrainProtectionIsPresentEventually(instance[4], splitBrainProtectionId);
    }

    private void verifySplitBrainProtections(String splitBrainProtectionId) {
        assertSplitBrainProtectionIsPresentEventually(instance[0], splitBrainProtectionId);
        assertSplitBrainProtectionIsPresentEventually(instance[1], splitBrainProtectionId);
        assertSplitBrainProtectionIsPresentEventually(instance[2], splitBrainProtectionId);
        assertSplitBrainProtectionIsAbsentEventually(instance[3], splitBrainProtectionId);
        assertSplitBrainProtectionIsAbsentEventually(instance[4], splitBrainProtectionId);
    }

    private void assertSplitBrainProtectionIsPresentEventually(final HazelcastInstance instance, final String splitBrainProtectionId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(instance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionId).hasMinimumSize());
            }
        });
    }

    private void assertSplitBrainProtectionIsAbsentEventually(final HazelcastInstance instance, final String splitBrainProtectionId) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertFalse(instance.getSplitBrainProtectionService().getSplitBrainProtection(splitBrainProtectionId).hasMinimumSize());
            }
        });
    }
}
