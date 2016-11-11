package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.Callable;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterVersionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HazelcastInstance instance;
    private ClusterServiceImpl cluster;
    private Version codebaseVersion;

    @Test
    public void test_clusterVersion_isEventuallySet_whenSingleNodeMulticastJoinerCluster() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        setupInstance(config);
        assertEqualsEventually(new Callable<Version>() {
            @Override
            public Version call()
                    throws Exception {
                return cluster.getClusterVersion();
            }
        }, codebaseVersion);
    }

    @Test
    public void test_clusterVersion_isEventuallySet_whenNoJoinerConfiguredSingleNode() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        setupInstance(config);
        assertEqualsEventually(new Callable<Version>() {
            @Override
            public Version call()
                    throws Exception {
                return cluster.getClusterVersion();
            }
        }, codebaseVersion);
    }

    @Test
    public void test_clusterVersion_isEventuallySet_whenTcpJoinerConfiguredSingleNode() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        setupInstance(config);
        assertEqualsEventually(new Callable<Version>() {
            @Override
            public Version call()
                    throws Exception {
                return cluster.getClusterVersion();
            }
        }, codebaseVersion);
    }

    @Test
    public void test_clusterVersion_isEventuallySetOnJoiningMember_whenMulticastJoinerConfigured() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        setupInstance(config);
        HazelcastInstance joiner = Hazelcast.newHazelcastInstance(config);
        final ClusterServiceImpl joinerCluster = (ClusterServiceImpl) joiner.getCluster();

        assertEqualsEventually(new Callable<Version>() {
            @Override
            public Version call()
                    throws Exception {
                return joinerCluster.getClusterVersion();
            }
        }, codebaseVersion);

        joiner.shutdown();
    }

    @Test
    public void test_clusterVersion_isEventuallySetOnJoiningMember_whenTcpJoinerConfigured() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        setupInstance(config);
        HazelcastInstance joiner = Hazelcast.newHazelcastInstance(config);
        final ClusterServiceImpl joinerCluster = (ClusterServiceImpl) joiner.getCluster();

        assertEqualsEventually(new Callable<Version>() {
            @Override
            public Version call()
                    throws Exception {
                return joinerCluster.getClusterVersion();
            }
        }, codebaseVersion);

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
