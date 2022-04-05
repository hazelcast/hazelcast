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

package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static org.junit.Assert.assertEquals;

/**
 * Utilities for cluster upgrade tests: start cluster members at designated version, assert cluster version etc.
 */
@SuppressWarnings("unused")
public final class TestClusterUpgradeUtils {

    private TestClusterUpgradeUtils() {
    }

    // return a new HazelcastInstance at given version
    public static HazelcastInstance newHazelcastInstance(TestHazelcastInstanceFactory factory, MemberVersion version,
                                                         Config config) {
        try {
            System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, version.toString());
            return factory.newHazelcastInstance(config);
        } finally {
            System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        }
    }

    // shutdown and replace each member in membersToUpgrade with a new HazelcastInstance at given version
    public static void upgradeClusterMembers(TestHazelcastInstanceFactory factory, HazelcastInstance[] membersToUpgrade,
                                             MemberVersion version, Config config) {
        upgradeClusterMembers(factory, membersToUpgrade, version, config, true);
    }

    public static void upgradeClusterMembers(TestHazelcastInstanceFactory factory, final HazelcastInstance[] membersToUpgrade,
                                             MemberVersion version, Config config, boolean assertClusterSize) {
        try {
            System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, version.toString());
            // upgrade one by one each member of the cluster to the next version
            for (int i = 0; i < membersToUpgrade.length; i++) {
                membersToUpgrade[i].shutdown();
                waitAllForSafeState(membersToUpgrade);
                // if new node's version is incompatible, then node startup will fail with IllegalStateException
                membersToUpgrade[i] = factory.newHazelcastInstance(config);
                waitAllForSafeState(membersToUpgrade);
                if (assertClusterSize) {
                    // assert all members are in the cluster
                    assertTrueEventually(()
                            -> assertEquals(membersToUpgrade[0].toString(),
                            membersToUpgrade.length, membersToUpgrade[0].getCluster().getMembers().size()));
                }
            }
        } finally {
            System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
        }
    }

    // assert all members' clusterService reports the given version
    public static void assertClusterVersion(HazelcastInstance[] instances, Version version) {
        for (HazelcastInstance instance : instances) {
            assertEquals(version, instance.getCluster().getClusterVersion());
        }
    }

    // assert all nodes in the cluster have the given codebase version
    public static void assertNodesVersion(HazelcastInstance[] instances, MemberVersion version) {
        for (HazelcastInstance instance : instances) {
            assertEquals(version, getNode(instance).getVersion());
        }
    }
}
