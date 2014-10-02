package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastTestSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractJoinTest extends HazelcastTestSupport {

    protected void testJoin(Config config) throws Exception {
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        assertEquals(1, h1.getCluster().getMembers().size());

        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());

        h1.shutdown();
        h1 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(2, h1);
        assertClusterSize(2, h2);
    }

    protected void testJoin_With_DifferentBuildNumber(Config config) {
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");

        String buildNumberProp = "hazelcast.build";
        System.setProperty(buildNumberProp, "1");
        try {
            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);

            System.setProperty(buildNumberProp, "2");
            HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

            assertClusterSize(2, h1);
            assertClusterSize(2, h2);
        } finally {
            System.clearProperty(buildNumberProp);
        }
    }

    /**
     * Checks if a HazelcastInstance created with config2, can be added to a HazelcastInstance created with config 1.
     * <p/>
     * This method expects that an IllegalStateException is thrown when the second HazelcastInstance is created and
     * it doesn't join the cluster but gets killed instead.
     *
     * @param config1
     * @param config2
     */
    protected void assertIncompatible(Config config1, Config config2) {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config1);

        try {
            Hazelcast.newHazelcastInstance(config2);
            fail();
        } catch (IllegalStateException e) {

        }

        assertTrue(hz1.getLifecycleService().isRunning());
        assertClusterSize(1, hz1);
    }

    protected void assertIndependentClusters(Config config1, Config config2) {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config2);

        assertTrue(hz1.getLifecycleService().isRunning());
        assertClusterSize(1, hz1);

        assertTrue(hz2.getLifecycleService().isRunning());
        assertClusterSize(1, hz2);
    }
}
