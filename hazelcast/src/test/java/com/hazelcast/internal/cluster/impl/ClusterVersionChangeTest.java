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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.Accessors.getNode;

/**
 * Test cluster version transitions
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterVersionChangeTest
        extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private HazelcastInstance instance;
    private ClusterServiceImpl cluster;
    private CountDownLatch clusterVersionUpgradeLatch;
    private MemberVersion codebaseVersion;

    @Before
    public void setup() {
        instance = createHazelcastInstance();
        cluster = (ClusterServiceImpl) instance.getCluster();
        // expecting countdown twice: once upon listener registration, once more on version upgrade
        clusterVersionUpgradeLatch = new CountDownLatch(2);
        getNode(instance).getNodeExtension().registerListener(new ClusterVersionChangedListener(clusterVersionUpgradeLatch));
        codebaseVersion = getNode(instance).getVersion();
    }

    @Test
    public void test_clusterVersionUpgradeFails_whenNodeMajorVersionPlusOne() {
        Version newVersion = Version.of(codebaseVersion.getMajor() + 1, codebaseVersion.getMinor());

        expectedException.expect(VersionMismatchException.class);
        cluster.changeClusterVersion(newVersion);
    }

    @Test
    public void test_clusterVersionUpgradeFails_whenNodeMinorVersionPlusOne() {
        Version newVersion = Version.of(codebaseVersion.getMajor(), codebaseVersion.getMinor() + 1);

        expectedException.expect(VersionMismatchException.class);
        cluster.changeClusterVersion(newVersion);
    }

    public static class ClusterVersionChangedListener implements ClusterVersionListener {
        private final CountDownLatch latch;

        public ClusterVersionChangedListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onClusterVersionChange(Version newVersion) {
            latch.countDown();
        }
    }
}
