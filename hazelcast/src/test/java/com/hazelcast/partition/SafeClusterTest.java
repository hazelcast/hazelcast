package com.hazelcast.partition;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Includes tests for node and cluster safety.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SafeClusterTest extends HazelcastTestSupport {

    private static final String DUMMY_MIGRATION_SERVICE = "dummy_migration_service";

    @Test
    public void isClusterSafe() throws Exception {
        final HazelcastInstance node = createHazelcastInstance();
        final boolean safe = node.getPartitionService().isClusterSafe();

        assertTrue(safe);
    }

    //isClusterSafe_whenMemberListEmpty and isClusterSafe_whenMemberListNull are not testable.

    @Test
    public void isClusterSafe_multiNode() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = factory.newHazelcastInstance();
        final HazelcastInstance node2 = factory.newHazelcastInstance();
        final boolean safe1 = node1.getPartitionService().isClusterSafe();
        final boolean safe2 = node2.getPartitionService().isClusterSafe();

        assertTrue(safe1);
        assertTrue(safe2);
    }

    @Test
    public void isClusterSafe_whenMigration() {
        final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = factory.newHazelcastInstance(createConfigWithDummyMigrationService());

        IMap<Object, Object> map = node1.getMap("migrationMap");
        for (int i = 0; i < 100; i++) {
            map.put("key" + i, "value");
        }
        SimpleMigrationListener migrationListener = new SimpleMigrationListener(1, 1);
        node1.getPartitionService().addMigrationListener(migrationListener);
        final HazelcastInstance node2 = factory.newHazelcastInstance(createConfigWithDummyMigrationService());
        assertOpenEventually(migrationListener.startLatch);
        //we can not make sure whether migration is complete at this time gap.
        //for this reason we used DummyMigrationService
        boolean clusterSafe = node2.getPartitionService().isClusterSafe();
        assertFalse(clusterSafe);
    }

    @Test(expected = NullPointerException.class)
    public void isMemberSafe_whenMemberNull() {
        HazelcastInstance node = createHazelcastInstance();
        boolean safe = node.getPartitionService().isMemberSafe(null);

        assertFalse(safe);
    }

    @Test
    public void isMemberSafe_localMember() throws Exception {
        final HazelcastInstance node = createHazelcastInstance();
        final Member localMember = node.getCluster().getLocalMember();
        final boolean safe = node.getPartitionService().isMemberSafe(localMember);

        assertTrue(safe);
    }

    @Test
    public void isMemberSafe_multiNode() {
        final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = factory.newHazelcastInstance();
        final HazelcastInstance node2 = factory.newHazelcastInstance();
        Member member = node2.getCluster().getLocalMember();
        boolean safe = node1.getPartitionService().isMemberSafe(member);

        assertTrue(safe);
    }

    @Test
    public void isMemberSafe_whenNodeShutdown() {
        final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = factory.newHazelcastInstance();
        final HazelcastInstance node2 = factory.newHazelcastInstance();
        Member member = node2.getCluster().getLocalMember();
        node2.shutdown();
        boolean safe = node1.getPartitionService().isMemberSafe(member);
        assertFalse(safe);
    }

    @Test
    public void isMemberSafe_whenMigration() {
        final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = factory.newHazelcastInstance(createConfigWithDummyMigrationService());

        IMap<Object, Object> map = node1.getMap("migrationMap");
        for (int i = 0; i < 100; i++) {
            map.put("key" + i, "value");
        }
        SimpleMigrationListener migrationListener = new SimpleMigrationListener(1, 1);
        node1.getPartitionService().addMigrationListener(migrationListener);
        HazelcastInstance node2 = factory.newHazelcastInstance(createConfigWithDummyMigrationService());
        assertOpenEventually(migrationListener.startLatch);
        Member member = node1.getCluster().getLocalMember();
        final boolean safe = node2.getPartitionService().isMemberSafe(member);
        assertFalse(safe);
    }

    @Test
    public void isLocalMemberSafe() throws Exception {
        final HazelcastInstance node = createHazelcastInstance();
        final boolean safe = node.getPartitionService().isLocalMemberSafe();

        assertTrue(safe);
    }

    @Test
    public void isLocalMemberSafe_multiNode() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = factory.newHazelcastInstance();
        final HazelcastInstance node2 = factory.newHazelcastInstance();
        final boolean safe1 = node1.getPartitionService().isLocalMemberSafe();
        final boolean safe2 = node2.getPartitionService().isLocalMemberSafe();

        assertTrue(safe1);
        assertTrue(safe2);
    }

    @Test
    public void isLocalMemberSafe_nodeNotActive() {
        HazelcastInstance node = createHazelcastInstance();
        getNode(node).setActive(false);
        boolean safe = node.getPartitionService().isLocalMemberSafe();

        assertTrue(safe);
    }

    @Test
    public void forceLocalMemberToBeSafe() throws Exception {
        final HazelcastInstance node = createHazelcastInstance();
        final boolean safe = node.getPartitionService().forceLocalMemberToBeSafe(5, TimeUnit.SECONDS);

        assertTrue(safe);
    }

    @Test(expected = NullPointerException.class)
    public void forceLocalMemberToBeSafe_whenUnitNull() {
        final HazelcastInstance node = createHazelcastInstance();
        final boolean safe = node.getPartitionService().forceLocalMemberToBeSafe(5, null);
        assertFalse(safe);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forceLocalMemberToBeSafe_whenTimeoutIllegal() {
        final HazelcastInstance node = createHazelcastInstance();
        final boolean safe = node.getPartitionService().forceLocalMemberToBeSafe(0, TimeUnit.SECONDS);
        assertFalse(safe);
    }

    @Test
    public void forceLocalMemberToBeSafe_whenNodeNotActive() {
        HazelcastInstance node = createHazelcastInstance();
        getNode(node).setActive(false);
        boolean safe = node.getPartitionService().forceLocalMemberToBeSafe(2, TimeUnit.SECONDS);

        assertTrue(safe);
    }

    class SimpleMigrationListener implements MigrationListener {

        CountDownLatch startLatch;
        CountDownLatch completeLatch;

        SimpleMigrationListener(int startCount, int completeCount) {
            startLatch = new CountDownLatch(startCount);
            completeLatch = new CountDownLatch(completeCount);
        }

        @Override
        public void migrationStarted(MigrationEvent migrationEvent) {
            startLatch.countDown();
        }

        @Override
        public void migrationCompleted(MigrationEvent migrationEvent) {
            completeLatch.countDown();
        }

        @Override
        public void migrationFailed(MigrationEvent migrationEvent) {

        }
    }

    private Config createConfigWithDummyMigrationService() {
        Config config = new Config();
        ServicesConfig servicesConfig = config.getServicesConfig();
        servicesConfig.addServiceConfig(new ServiceConfig().setName(DUMMY_MIGRATION_SERVICE)
                .setEnabled(true).setServiceImpl(new DummyMigrationService(DUMMY_MIGRATION_SERVICE)));
        return config;
    }


    public static class DummyMigrationService implements MigrationAwareService {

        final String serviceName;

        public DummyMigrationService(String serviceName) {
            this.serviceName = serviceName;
        }

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            return null;
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
            LockSupport.parkNanos(5000 * 1000000);
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {

        }

        @Override
        public void clearPartitionReplica(int partitionId) {

        }
    }

}
