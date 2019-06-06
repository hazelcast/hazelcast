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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractJoinTest extends HazelcastTestSupport {

    protected void testJoin(Config config) throws Exception {
        config.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(1, h1);

        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(2, h1, h2);

        h1.shutdown();
        h1 = Hazelcast.newHazelcastInstance(config);
        // when h1 is returned, it's guaranteed that it should see 2 members
        assertClusterSize(2, h1);

        assertClusterSize(2, h2);
    }

    protected void testJoin_With_DifferentBuildNumber(Config config) {
        config.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");

        String buildNumberProp = "hazelcast.build";
        System.setProperty(buildNumberProp, "1");
        try {
            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);

            System.setProperty(buildNumberProp, "2");
            HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

            assertClusterSize(2, h1, h2);
        } finally {
            System.clearProperty(buildNumberProp);
        }
    }

    /**
     * Checks if a HazelcastInstance created with config2, can be added to a HazelcastInstance created with config 1.
     * <p>
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

    protected static void assertIndependentClustersAndDoNotMergedEventually(Config config1, Config config2, int durationSeconds) {
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config1);
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config2);

        assertTrue(hz1.getLifecycleService().isRunning());
        assertClusterSize(1, hz1);

        assertTrue(hz2.getLifecycleService().isRunning());
        assertClusterSize(1, hz2);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertClusterSize(1, hz1);
                assertClusterSize(1, hz2);
            }
        }, durationSeconds);
    }
}
