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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNode;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterVersionInitTest extends HazelcastTestSupport {

    private HazelcastInstance instance;
    private ClusterServiceImpl cluster;
    private MemberVersion codebaseVersion;

    @Test
    public void test_clusterVersion_isEventuallySet_whenSingleNodeMulticastJoinerCluster() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        setupInstance(config);
        assertEqualsEventually(() -> cluster.getClusterVersion(), codebaseVersion.asVersion());
    }

    @Test
    public void test_clusterVersion_isEventuallySet_whenNoJoinerConfiguredSingleNode() {
        Config config = new Config();
        config.setClusterName(randomName());
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        setupInstance(config);
        assertEqualsEventually(() -> cluster.getClusterVersion(), codebaseVersion.asVersion());
    }

    @Test
    public void test_clusterVersion_isEventuallySet_whenTcpJoinerConfiguredSingleNode() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        setupInstance(config);
        assertEqualsEventually(() -> cluster.getClusterVersion(), codebaseVersion.asVersion());
    }

    @Test
    public void test_clusterVersion_isEventuallySetOnJoiningMember_whenMulticastJoinerConfigured() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        setupInstance(config);
        HazelcastInstance joiner = Hazelcast.newHazelcastInstance(config);
        ClusterServiceImpl joinerCluster = (ClusterServiceImpl) joiner.getCluster();

        assertEqualsEventually(joinerCluster::getClusterVersion, codebaseVersion.asVersion());

        joiner.shutdown();
    }

    @Test
    public void test_clusterVersion_isEventuallySetOnJoiningMember_whenTcpJoinerConfigured() {
        Config config = new Config();
        config.setClusterName(randomName());
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        setupInstance(config);
        HazelcastInstance joiner = Hazelcast.newHazelcastInstance(config);
        ClusterServiceImpl joinerCluster = (ClusterServiceImpl) joiner.getCluster();

        assertEqualsEventually(joinerCluster::getClusterVersion, codebaseVersion.asVersion());

        joiner.shutdown();
    }

    private void setupInstance(Config config) {
        instance = Hazelcast.newHazelcastInstance(config);
        cluster = (ClusterServiceImpl) instance.getCluster();
        codebaseVersion = getNode(instance).getVersion();
    }

    @After
    public void tearDown() {
        instance.shutdown();
    }
}
