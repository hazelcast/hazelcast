package com.hazelcast.partition;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.PartitionService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class PartitionMigrationListenerTest extends HazelcastTestSupport {

    @Test(expected = NullPointerException.class)
    public void testAddMigrationListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.addMigrationListener(null);
    }

    @Test
    public void testAddMigrationListener_whenListenerRegisteredTwice() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        PartitionService partitionService = hz1.getPartitionService();

        MigrationListener listener = mock(MigrationListener.class);

        String id1 = partitionService.addMigrationListener(listener);
        String id2 = partitionService.addMigrationListener(listener);

        // first we check if the registration id's are different
        assertNotEquals(id1, id2);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveMigrationListener_whenNullListener() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        partitionService.removeMigrationListener(null);
    }

    @Test
    public void testRemoveMigrationListener_whenNonExistingRegistrationId() {
        HazelcastInstance hz = createHazelcastInstance();
        PartitionService partitionService = hz.getPartitionService();

        boolean result = partitionService.removeMigrationListener("notexist");

        assertFalse(result);
    }

    @Test
    public void testRemoveMigrationListener() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance hz1 = factory.newHazelcastInstance();
        PartitionService partitionService = hz1.getPartitionService();

        MigrationListener listener = mock(MigrationListener.class);

        String id = partitionService.addMigrationListener(listener);
        boolean removed = partitionService.removeMigrationListener(id);

        assertTrue(removed);

        // now we add a member
        HazelcastInstance hz2 = factory.newHazelcastInstance();
        warmUpPartitions(hz1, hz2);

        // and verify that the listener isn't called.
        verify(listener, never()).migrationStarted(any(MigrationEvent.class));
    }
}
