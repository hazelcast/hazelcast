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

package com.hazelcast.instance.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.internal.cluster.impl.ClusterStateManager;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.properties.ClusterProperty;
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

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.UuidUtil.newUnsecureUUID;
import static com.hazelcast.test.Accessors.getNode;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Test DefaultNodeExtension behavior
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultNodeExtensionTest extends HazelcastTestSupport {

    private int buildNumber;
    private HazelcastInstance hazelcastInstance;
    private Node node;
    private NodeExtension nodeExtension;
    private MemberVersion nodeVersion;
    private Address joinAddress;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        buildNumber = BuildInfoProvider.getBuildInfo().getBuildNumber();
        hazelcastInstance = createHazelcastInstance();
        nodeExtension = getNode(hazelcastInstance).getNodeExtension();
        node = getNode(hazelcastInstance);
        nodeVersion = node.getVersion();
        joinAddress = new Address("127.0.0.1", 9999);
    }

    @Test
    public void test_nodeVersionCompatibleWith_ownClusterVersion() {
        MemberVersion currentVersion = getNode(hazelcastInstance).getVersion();
        assertTrue(nodeExtension.isNodeVersionCompatibleWith(currentVersion.asVersion()));
    }

    @Test
    public void test_nodeVersionNotCompatibleWith_otherMinorVersion() {
        MemberVersion currentVersion = getNode(hazelcastInstance).getVersion();
        Version minorPlusOne = Version.of(currentVersion.getMajor(), currentVersion.getMinor() + 1);
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(minorPlusOne));
    }

    @Test
    public void test_nodeVersionNotCompatibleWith_otherMajorVersion() {
        MemberVersion currentVersion = getNode(hazelcastInstance).getVersion();
        Version majorPlusOne = Version.of(currentVersion.getMajor() + 1, currentVersion.getMinor());
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(majorPlusOne));
    }

    @Test
    public void test_joinRequestAllowed_whenSameVersion() {
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, buildNumber, nodeVersion, joinAddress, newUnsecureUUID(),
                false, null, null, null, null, null);

        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_joinRequestAllowed_whenNextPatchVersion() {
        MemberVersion nextPatchVersion = MemberVersion.of(nodeVersion.getMajor(), nodeVersion.getMinor(),
                nodeVersion.getPatch() + 1);
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, buildNumber, nextPatchVersion, joinAddress,
                newUnsecureUUID(), false, null, null, null, null, null);

        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_joinRequestFails_whenNextMinorVersion() {
        MemberVersion nextMinorVersion = MemberVersion.of(nodeVersion.getMajor(), nodeVersion.getMinor() + 1,
                nodeVersion.getPatch());
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, buildNumber, nextMinorVersion, joinAddress,
                newUnsecureUUID(), false, null, null, null, null, null);

        expected.expect(VersionMismatchException.class);
        expected.expectMessage(containsString("Rolling Member Upgrades are only supported in Hazelcast Enterprise"));
        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_joinRequestFails_whenPreviousMinorVersion() {
        assumeTrue("Minor version is 0", nodeVersion.getMinor() > 0);
        MemberVersion nextMinorVersion = MemberVersion.of(nodeVersion.getMajor(), nodeVersion.getMinor() - 1,
                nodeVersion.getPatch());
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, buildNumber, nextMinorVersion, joinAddress,
                newUnsecureUUID(), false, null, null, null, null, null);

        expected.expect(VersionMismatchException.class);
        expected.expectMessage(containsString("Rolling Member Upgrades are only supported for the next minor version"));
        expected.expectMessage(containsString("Rolling Member Upgrades are only supported in Hazelcast Enterprise"));
        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_joinRequestFails_whenNextMajorVersion() {
        MemberVersion nextMajorVersion = MemberVersion.of(nodeVersion.getMajor() + 1, nodeVersion.getMinor(),
                nodeVersion.getPatch());
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, buildNumber, nextMajorVersion, joinAddress,
                newUnsecureUUID(), false, null, null, null, null, null);

        expected.expect(VersionMismatchException.class);
        expected.expectMessage(containsString("Rolling Member Upgrades are only supported for the same major version"));
        expected.expectMessage(containsString("Rolling Member Upgrades are only supported in Hazelcast Enterprise"));
        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_joinRequestFails_whenPreviousMajorVersion() {
        MemberVersion prevMajorVersion = MemberVersion.of(nodeVersion.getMajor() - 1, nodeVersion.getMinor(),
                nodeVersion.getPatch());
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, buildNumber, prevMajorVersion, joinAddress,
                newUnsecureUUID(), false, null, null, null, null, null);

        expected.expect(VersionMismatchException.class);
        expected.expectMessage(containsString("Rolling Member Upgrades are only supported for the same major version"));
        expected.expectMessage(containsString("Rolling Member Upgrades are only supported in Hazelcast Enterprise"));
        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_clusterVersionListener_invokedOnRegistration() {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterVersionListener listener = newVersion -> latch.countDown();
        assertTrue(nodeExtension.registerListener(listener));
        assertOpenEventually(latch);
    }

    @Test
    public void test_listenerNotRegistered_whenUnknownType() {
        assertFalse(nodeExtension.registerListener(new Object()));
    }

    @Test
    public void test_listenerHazelcastInstanceInjected_whenHazelcastInstanceAware() {
        HazelcastInstanceAwareVersionListener listener = new HazelcastInstanceAwareVersionListener();
        assertTrue(nodeExtension.registerListener(listener));
        assertEquals(hazelcastInstance, listener.getInstance());
    }

    @Test
    public void test_clusterVersionListener_invokedWithNodeCodebaseVersion_whenClusterVersionIsNull() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean(false);
        ClusterVersionListener listener = newVersion -> {
            if (!newVersion.equals(nodeVersion.asVersion())) {
                failed.set(true);
            }
            latch.countDown();
        };
        makeClusterVersionUnknownAndVerifyListener(latch, failed, listener);
    }

    @Test
    public void test_clusterVersionListener_invokedWithOverriddenPropertyValue_whenClusterVersionIsNull() throws Exception {
        // override initial cluster version
        System.setProperty(ClusterProperty.INIT_CLUSTER_VERSION.getName(), "2.1.7");
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean(false);
        ClusterVersionListener listener = newVersion -> {
            if (!newVersion.equals(Version.of("2.1.7"))) {
                failed.set(true);
            }
            latch.countDown();
        };
        makeClusterVersionUnknownAndVerifyListener(latch, failed, listener);
        System.clearProperty(ClusterProperty.INIT_CLUSTER_VERSION.getName());
    }

    private void makeClusterVersionUnknownAndVerifyListener(CountDownLatch latch, AtomicBoolean failed,
                                                            ClusterVersionListener listener) throws Exception {
        // directly set clusterVersion field's value to null
        Field setClusterVersionMethod = ClusterStateManager.class.getDeclaredField("clusterVersion");
        setClusterVersionMethod.setAccessible(true);
        setClusterVersionMethod.set(node.getClusterService().getClusterStateManager(), Version.UNKNOWN);
        // register listener and assert it's successful
        assertTrue(nodeExtension.registerListener(listener));
        // listener was executed
        assertOpenEventually(latch);
        // clusterVersion field's value was actually null
        assertTrue(node.getClusterService().getClusterStateManager().getClusterVersion().isUnknown());
        // listener received node's codebase version as new cluster version
        assertFalse(failed.get());
    }

    public static class HazelcastInstanceAwareVersionListener implements ClusterVersionListener, HazelcastInstanceAware {

        private HazelcastInstance instance;

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instance = hazelcastInstance;
        }

        @Override
        public void onClusterVersionChange(Version newVersion) {
        }

        public HazelcastInstance getInstance() {
            return instance;
        }
    }
}
