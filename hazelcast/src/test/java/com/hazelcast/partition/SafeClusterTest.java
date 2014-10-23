package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

// TODO tests are not sufficient. add more tests.
/**
 * Includes tests for node and cluster safety.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SafeClusterTest extends HazelcastTestSupport {

    @Test
    public void isClusterSafe() throws Exception {
        final HazelcastInstance node = createHazelcastInstance();
        final boolean safe = node.getPartitionService().isClusterSafe();

        assertTrue(safe);
    }

    @Test
    public void isClusterSafe_multiNode() throws Exception {
        final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = factory.newHazelcastInstance();
        final HazelcastInstance node2 = factory.newHazelcastInstance();
        final boolean safe1 = node1.getPartitionService().isClusterSafe();
        final boolean safe2 = node2.getPartitionService().isClusterSafe();

        assertTrue(safe1);
        assertTrue(safe2);
    }

    @Test
    public void isLocalMemberSafe() throws Exception {
        final HazelcastInstance node = createHazelcastInstance();
        final boolean safe = node.getPartitionService().isLocalMemberSafe();

        assertTrue(safe);
    }

    @Test
    public void isLocalMemberSafe_multiNode() throws Exception {
        final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = factory.newHazelcastInstance();
        final HazelcastInstance node2 = factory.newHazelcastInstance();
        final boolean safe1 = node1.getPartitionService().isLocalMemberSafe();
        final boolean safe2 = node2.getPartitionService().isLocalMemberSafe();

        assertTrue(safe1);
        assertTrue(safe2);
    }

    @Test
    public void isMemberSafe_localMember() throws Exception {
        final HazelcastInstance node = createHazelcastInstance();
        final Member localMember = node.getCluster().getLocalMember();
        final boolean safe = node.getPartitionService().isMemberSafe(localMember);

        assertTrue(safe);
    }

    @Test
    public void test_forceLocalMemberToBeSafe() throws Exception {
        final HazelcastInstance node = createHazelcastInstance();
        final boolean safe = node.getPartitionService().forceLocalMemberToBeSafe(5, TimeUnit.SECONDS);

        assertTrue(safe);
    }
}
