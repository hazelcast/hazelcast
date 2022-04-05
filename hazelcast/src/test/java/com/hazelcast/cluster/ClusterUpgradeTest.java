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

package com.hazelcast.cluster;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SerializationSamplesExcluded;
import com.hazelcast.version.MemberVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.test.TestClusterUpgradeUtils.upgradeClusterMembers;

/**
 * Create a cluster, then change cluster version. This test uses artificial version numbers, to avoid relying on current version.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, SerializationSamplesExcluded.class})
public class ClusterUpgradeTest extends HazelcastTestSupport {

    static final MemberVersion VERSION_2_0_5 = MemberVersion.of(2, 0, 5);
    static final MemberVersion VERSION_2_1_0 = MemberVersion.of(2, 1, 0);
    static final MemberVersion VERSION_2_1_1 = MemberVersion.of(2, 1, 1);
    static final MemberVersion VERSION_2_2_0 = MemberVersion.of(2, 2, 0);
    static final MemberVersion VERSION_3_0_0 = MemberVersion.of(3, 0, 0);

    static final int CLUSTER_MEMBERS_COUNT = 3;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(10);
    private HazelcastInstance[] clusterMembers;
    private ClusterService clusterService;

    @Before
    public void setup() {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, VERSION_2_1_0.toString());
        clusterMembers = new HazelcastInstance[CLUSTER_MEMBERS_COUNT];
        for (int i = 0; i < CLUSTER_MEMBERS_COUNT; i++) {
            clusterMembers[i] = factory.newHazelcastInstance(getConfig());
        }
        clusterService = (ClusterService) clusterMembers[0].getCluster();
    }

    @Test
    public void test_upgradeMinorVersion_notAllowed() {
        expectedException.expect(IllegalStateException.class);
        upgradeCluster(VERSION_2_2_0);
    }

    @Test
    public void test_upgradeMajorVersion_notAllowed() {
        expectedException.expect(IllegalStateException.class);
        upgradeCluster(VERSION_3_0_0);
    }

    @Test
    public void test_addNodeOfLesserThanClusterVersion_notAllowed() {
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, VERSION_2_0_5.toString());
        expectedException.expect(IllegalStateException.class);
        factory.newHazelcastInstance(getConfig());
    }

    @Test
    public void test_changeClusterVersion_disallowedForMinorVersions() {
        expectedException.expect(VersionMismatchException.class);
        clusterService.changeClusterVersion(VERSION_2_0_5.asVersion());
    }

    void upgradeCluster(MemberVersion version) {
        upgradeClusterMembers(factory, clusterMembers, version, getConfig());
        // also update the reference to clusterService to the one from
        clusterService = (ClusterService) clusterMembers[0].getCluster();
    }

    @After
    public void tearDown() {
        System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
    }
}
