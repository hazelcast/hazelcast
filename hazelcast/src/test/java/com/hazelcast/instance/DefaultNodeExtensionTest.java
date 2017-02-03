package com.hazelcast.instance;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.internal.cluster.impl.ClusterStateManager;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.version.Version;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.version.MemberVersion;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.instance.BuildInfoProvider.BUILD_INFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test DefaultNodeExtension behavior
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class DefaultNodeExtensionTest extends HazelcastTestSupport {

    protected HazelcastInstance hazelcastInstance;
    protected Node node;
    protected NodeExtension nodeExtension;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void setup() throws Exception {
        hazelcastInstance = createHazelcastInstance();
        nodeExtension = getNode(hazelcastInstance).getNodeExtension();
        node = getNode(hazelcastInstance);
    }

    @Test
    public void test_nodeVersionCompatibleWith_ownClusterVersion() {
        MemberVersion currentVersion = getNode(hazelcastInstance).getVersion();
        assertTrue(nodeExtension.isNodeVersionCompatibleWith(currentVersion.asVersion()));
    }

    @Test
    public void test_nodeVersionNotCompatibleWith_otherMinorVersion() {
        MemberVersion currentVersion = getNode(hazelcastInstance).getVersion();
        Version minorMinusOne = Version.of(currentVersion.getMajor(), currentVersion.getMinor() - 1);
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(minorMinusOne));
    }

    @Test
    public void test_nodeVersionNotCompatibleWith_otherMajorVersion() {
        MemberVersion currentVersion = getNode(hazelcastInstance).getVersion();
        Version majorPlusOne = Version.of(currentVersion.getMajor() + 1, currentVersion.getMinor());
        assertFalse(nodeExtension.isNodeVersionCompatibleWith(majorPlusOne));
    }

    @Test
    public void test_joinRequestAllowed_whenSameVersion()
            throws UnknownHostException {
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, BUILD_INFO.getBuildNumber(), node.getVersion(),
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, null, null);
        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_joinRequestAllowed_whenOtherPatchVersion()
            throws UnknownHostException {
        MemberVersion otherPatchVersion = MemberVersion
                .of(node.getVersion().getMajor(), node.getVersion().getMinor(), node.getVersion().getPatch() + 1);
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, BUILD_INFO.getBuildNumber(), otherPatchVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, null, null);
        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_joinRequestFails_whenOtherMinorVersion()
            throws UnknownHostException {
        MemberVersion otherPatchVersion = MemberVersion
                .of(node.getVersion().getMajor(), node.getVersion().getMinor() + 1, node.getVersion().getPatch());
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, BUILD_INFO.getBuildNumber(), otherPatchVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, null, null);
        expected.expect(VersionMismatchException.class);
        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_joinRequestFails_whenOtherMajorVersion()
            throws UnknownHostException {
        MemberVersion otherPatchVersion = MemberVersion
                .of(node.getVersion().getMajor() + 1, node.getVersion().getMinor(), node.getVersion().getPatch());
        JoinRequest joinRequest = new JoinRequest(Packet.VERSION, BUILD_INFO.getBuildNumber(), otherPatchVersion,
                new Address("127.0.0.1", 9999), UuidUtil.newUnsecureUuidString(), false, null, null, null, null);
        expected.expect(VersionMismatchException.class);
        nodeExtension.validateJoinRequest(joinRequest);
    }

    @Test
    public void test_clusterVersionListener_invokedOnRegistration()
            throws UnknownHostException {
        final CountDownLatch latch = new CountDownLatch(1);
        ClusterVersionListener listener = new ClusterVersionListener() {
            @Override
            public void onClusterVersionChange(Version newVersion) {
                latch.countDown();
            }
        };
        assertTrue(nodeExtension.registerListener(listener));
        assertOpenEventually(latch);
    }

    @Test
    public void test_listenerNotRegistered_whenUnknownType()
            throws UnknownHostException {
        assertFalse(nodeExtension.registerListener(new Object()));
    }

    @Test
    public void test_listenerHazelcastInstanceInjected_whenHazelcastInstanceAware() {
        HazelcastInstanceAwareVersionListener listener = new HazelcastInstanceAwareVersionListener();
        assertTrue(nodeExtension.registerListener(listener));
        assertEquals(hazelcastInstance, listener.getInstance());
    }

    @Test
    public void test_clusterVersionListener_invokedWithNodeCodebaseVersion_whenClusterVersionIsNull()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean(false);
        ClusterVersionListener listener = new ClusterVersionListener() {
            @Override
            public void onClusterVersionChange(Version newVersion) {
                if (!newVersion.equals(node.getVersion().asVersion())) {
                    failed.set(true);
                }
                latch.countDown();
            }
        };
        nullifyClusterVersionAndVerifyListener(latch, failed, listener);
    }

    @Test
    public void test_clusterVersionListener_invokedWithOverriddenPropertyValue_whenClusterVersionIsNull()
            throws UnknownHostException, NoSuchFieldException, IllegalAccessException {
        // override initial cluster version
        System.setProperty(GroupProperty.INIT_CLUSTER_VERSION.getName(), "2.1.7");
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean(false);
        ClusterVersionListener listener = new ClusterVersionListener() {
            @Override
            public void onClusterVersionChange(Version newVersion) {
                if (!newVersion.equals(Version.of("2.1.7"))) {
                    failed.set(true);
                }
                latch.countDown();
            }
        };
        nullifyClusterVersionAndVerifyListener(latch, failed, listener);
        System.clearProperty(GroupProperty.INIT_CLUSTER_VERSION.getName());
    }

    private void nullifyClusterVersionAndVerifyListener(CountDownLatch latch, AtomicBoolean failed,
                                                        ClusterVersionListener listener)
            throws NoSuchFieldException, IllegalAccessException {
        // directly set clusterVersion field's value to null
        Field setClusterVersionMethod = ClusterStateManager.class.getDeclaredField("clusterVersion");
        setClusterVersionMethod.setAccessible(true);
        setClusterVersionMethod.set(node.getClusterService().getClusterStateManager(), null);
        // register listener and assert it's successful
        assertTrue(nodeExtension.registerListener(listener));
        // listener was executed
        assertOpenEventually(latch);
        // clusterVersion field's value was actually null
        assertNull(node.getClusterService().getClusterStateManager().getClusterVersion());
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