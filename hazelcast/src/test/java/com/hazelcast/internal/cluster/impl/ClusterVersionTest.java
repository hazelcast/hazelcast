package com.hazelcast.internal.cluster.impl;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.version.Version;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterVersionTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void checkClusterVersion() {
        Cluster cluster = createSingleInstance("3.8.0");

        Version version = cluster.getClusterVersion();
        assertEquals(Version.of("3.8.0"), version);
    }

    @Test
    public void setClusterVersion_toCorrectMinorOne_succeeded() {
        Cluster cluster = createSingleInstance("3.8.0");

        cluster.changeClusterVersion(Version.of("3.7.0"));
        assertClusterVersion(cluster, "3.7.0");
    }

    @Test
    public void setClusterVersion_toWrongMinorOne_failed() {
        Cluster cluster = createSingleInstance("3.8.0");

        expected.expect(IllegalArgumentException.class);
        cluster.changeClusterVersion(Version.of("3.9.0"));
    }

    @Test
    public void setClusterVersion_tooHighMajorOne_failed() {
        Cluster cluster = createSingleInstance("3.8.0");

        expected.expect(IllegalArgumentException.class);
        cluster.changeClusterVersion(Version.of("4.0.0"));
    }

    @Test
    public void setClusterVersion_tooLowMajorOne_failed() {
        Cluster cluster = createSingleInstance("3.8.0");

        expected.expect(IllegalArgumentException.class);
        cluster.changeClusterVersion(Version.of("2.0.0"));
    }

    @Test
    public void setClusterVersion_shouldNotSetPatchVersion_failed() {
        Cluster cluster = createSingleInstance("3.8.0");

        expected.expect(IllegalArgumentException.class);
        cluster.changeClusterVersion(Version.of("3.8.1"));
    }

    @Test
    public void setClusterVersion_patchVersionCanBeOmitted_succeeded() {
        Cluster cluster = createSingleInstance("3.8.0");

        cluster.changeClusterVersion(Version.of("3.7"));
        assertClusterVersion(cluster, "3.7");
        assertClusterVersion(cluster, "3.7.0");
    }

    @Test
    public void setClusterVersion_twoNodes_emulatedLowerNode() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = createInstance(factory, "3.8.0");
        HazelcastInstance instance2 = createInstance(factory, "3.8.0");

        instance1.getCluster().changeClusterVersion(Version.of("3.7"));

        assertClusterVersion(instance1.getCluster(), "3.7");
        assertClusterVersion(instance2.getCluster(), "3.7");
    }

    @Test
    public void setClusterVersion_twoNodes_theSecondCantJoin_dueToHigherClusterVersion() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        createInstance(factory, "3.8.0");

        expected.expect(IllegalStateException.class);
        expected.expectMessage("Node failed to start");
        createInstance(factory, "3.7.0");
    }

    @Test
    public void setClusterVersion_twoNodes_theSecondCanJoin_dueToEmulatedMode() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = createInstance(factory, "3.7.0");
        HazelcastInstance instance2 = createInstance(factory, "3.8.0");

        assertClusterVersion(instance1.getCluster(), "3.7");
        assertClusterVersion(instance2.getCluster(), "3.7");
    }

    @Test
    public void setClusterVersion_twoNodes_cantUpgradeClusterVersion_validatedLocally() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = createInstance(factory, "3.7.0");
        HazelcastInstance instance2 = createInstance(factory, "3.8.0");

        assertClusterVersion(instance1.getCluster(), "3.7");
        assertClusterVersion(instance2.getCluster(), "3.7");

        expected.expect(IllegalArgumentException.class);
        expected.expectMessage("Node's codebase version 3.7.0 is incompatible with the requested version 3.8.0");
        instance1.getCluster().changeClusterVersion(Version.of("3.8"));
    }

    @Test
    public void setClusterVersion_twoNodes_cantUpgradeClusterVersion_validatedRemotelyOnTheIncompatibleNode() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = createInstance(factory, "3.7.0");
        HazelcastInstance instance2 = createInstance(factory, "3.8.0");

        assertClusterVersion(instance1.getCluster(), "3.7");
        assertClusterVersion(instance2.getCluster(), "3.7");

        expected.expect(IllegalArgumentException.class);
        expected.expectMessage("Node's codebase version 3.7.0 is incompatible with the requested version 3.8.0");
        instance2.getCluster().changeClusterVersion(Version.of("3.8"));
    }

    @Test(timeout = 60000)
    @Repeat(10)
    public void setClusterVersion_noRaceBetween_join() throws InterruptedException {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        // start two 3.8 nodes
        HazelcastInstance instance1 = createInstance(factory, "3.8.0");
        HazelcastInstance instance2 = createInstance(factory, "3.8.0");

        // change the cluster version to 3.7
        instance1.getCluster().changeClusterVersion(Version.of("3.7"));
        assertClusterVersion(instance1.getCluster(), "3.7");
        assertClusterVersion(instance2.getCluster(), "3.7");

        // start a 3.7 node and make it join the 3.7 emulated cluster
        final Semaphore semaphore = new Semaphore(1, true);
        final Semaphore shutdown = new Semaphore(1, true);
        semaphore.acquire();
        new Thread() {
            public void run() {
                shutdown.acquireUninterruptibly();
                HazelcastInstance instance3 = null;
                try {
                    instance3 = createInstance(factory, "3.7.0");
                } finally {
                    semaphore.acquireUninterruptibly();
                    System.err.println("Shutting down the third node");
                    if (instance3 != null) {
                        instance3.shutdown();
                    }
                    shutdown.release();
                }
            }
        }.start();

        // then wait a bit randomly
        Thread.sleep(new Random().nextInt(75));


        try {
            // and try to change the cluster version to 3.8
            // in this way
            instance1.getCluster().changeClusterVersion(Version.of("3.8"));
        } catch (Exception exception) {
            // EITHER ...
            // ... the change failed and the node joined
            assertTrue(exception instanceof IllegalStateException || exception instanceof IllegalArgumentException);
            // ... the 3.7.0 node HAS joined
            assertEquals(3, instance1.getCluster().getMembers().size());
            // ... cluster version is 3.7.0
            assertClusterVersion(instance1.getCluster(), "3.7");
            assertClusterVersion(instance2.getCluster(), "3.7");

            return;
        }

        // OR ...
        // ... the protocol version is 3.8
        assertClusterVersion(instance1.getCluster(), "3.8");
        assertClusterVersion(instance2.getCluster(), "3.8");
        // ... the 3.7.0 node has NOT joined
        assertEquals(2, instance1.getCluster().getMembers().size());

        // make sure that the third instance started in a separate thread stops
        semaphore.release();
        shutdown.acquireUninterruptibly();
    }

    private Cluster createSingleInstance(String version) {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = createInstance(factory, version);
        return instance.getCluster();
    }

    private static void assertClusterVersion(final Cluster cluster, String version) {
        assertEquals(cluster.getClusterVersion(), Version.of(version));
    }

    HazelcastInstance createInstance(TestHazelcastInstanceFactory factory, String requestedVersion) {
        String currentVersion = System.getProperty("hazelcast.version");
        System.setProperty("hazelcast.version", requestedVersion);
        HazelcastInstanceProxy instance = (HazelcastInstanceProxy) factory.newHazelcastInstance();
        if (currentVersion != null) {
            System.setProperty("hazelcast.version", currentVersion);
        } else {
            System.clearProperty("hazelcast.version");
        }
        return instance;
    }

}
