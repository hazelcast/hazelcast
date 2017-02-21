package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;


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
        Config config1 = newConfig();
        Config config2 = newConfig();

        int testDurationSeconds = 30;
        assertIndependentClustersAndDoNotMergedEventually(config1, config2, testDurationSeconds);
    }

    private Config newConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        config.getNetworkConfig().getJoin().getMulticastConfig().addTrustedInterface("8.8.8.8");
        return config;
    }
}
