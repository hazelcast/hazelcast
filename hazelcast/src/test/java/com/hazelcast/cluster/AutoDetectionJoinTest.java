package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AutoDetectionJoinTest extends AbstractJoinTest {

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void defaultConfig() throws Exception {
        testJoinWithDefaultWait(new Config());
    }

    @Test
    public void interfacesEnabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface(pickLocalInetAddress().getHostAddress());
        testJoinWithDefaultWait(config);
    }

    @Test
    public void differentClusterNames() throws Exception {
        Config config1 = new Config();
        config1.setClusterName("cluster1");

        Config config2 = new Config();
        config2.setClusterName("cluster2");

        assertIndependentClusters(config1, config2);
    }

    @Test
    public void autoDetectionDisabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);

        assertIndependentClusters(config, config);
    }

    @Test
    public void notUsedWhenOtherDiscoveryEnabled() throws Exception {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);

        assertIndependentClusters(config, config);
    }


}
