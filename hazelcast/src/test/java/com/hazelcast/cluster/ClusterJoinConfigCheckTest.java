package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClusterJoinConfigCheckTest {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testWrongPartitionCount() {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_PARTITION_COUNT, "100");

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_PARTITION_COUNT, "200");

        assertIncompatible(config1, config2);
    }

    @Test
    public void testWrongApplicationValidationToken() {
        Config config1 = new Config();
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        //config1.setProperty(GroupProperties.PROP_APPLICATION_VALIDATION_TOKEN, "foo");

        Config config2 = new Config();
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        //config2.setProperty(GroupProperties.PROP_APPLICATION_VALIDATION_TOKEN, "bar");

        assertIncompatible(config1, config2);
    }

    @Test
    public void testPartitionGroupEnabledDisabledMismatch() {
        Config config1 = new Config();
        config1.getPartitionGroupConfig().setEnabled(false);

        Config config2 = new Config();
        config2.getPartitionGroupConfig().setEnabled(true);

        assertIncompatible(config1, config2);
    }

    @Test
    public void testPartitionGroup_GroupTypeMismatch() {
        Config config1 = new Config();
        config1.getPartitionGroupConfig().setEnabled(true);
        config1.getPartitionGroupConfig().setGroupType(PartitionGroupConfig.MemberGroupType.CUSTOM);

        Config config2 = new Config();
        config2.getPartitionGroupConfig().setEnabled(true);
        config2.getPartitionGroupConfig().setGroupType(PartitionGroupConfig.MemberGroupType.PER_MEMBER);

        assertIncompatible(config1, config2);
    }

    private void assertIncompatible(Config config1, Config config2) {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config2);
    }
}
