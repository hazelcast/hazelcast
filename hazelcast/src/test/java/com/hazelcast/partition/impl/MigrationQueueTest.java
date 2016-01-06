package com.hazelcast.partition.impl;

import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Category(QuickTest.class)
public class MigrationQueueTest {

    private final MigrationQueue migrationQueue = new MigrationQueue();

    @Test
    public void test_migrationTaskCount_incremented_onMigrateTask() {
        migrationQueue.add(mock(InternalPartitionServiceImpl.MigrateTask.class));

        assertTrue(migrationQueue.hasMigrationTasks());
        assertEquals(1, migrationQueue.size());
    }

    @Test
    public void test_migrationTaskCount_notIncremented_onNonMigrateTask() {
        migrationQueue.add(mock(Runnable.class));

        assertFalse(migrationQueue.hasMigrationTasks());
        assertEquals(1, migrationQueue.size());
    }

    @Test
    public void test_migrationTaskCount_notDecremented_afterMigrateTaskPolled()
            throws InterruptedException {
        migrationQueue.add(mock(InternalPartitionServiceImpl.MigrateTask.class));
        migrationQueue.poll(1, TimeUnit.SECONDS);

        assertTrue(migrationQueue.hasMigrationTasks());
    }

    @Test
    public void test_migrateTaskCount_decremented_afterMigrateTaskCompleted()
            throws InterruptedException {
        final InternalPartitionServiceImpl.MigrateTask migrateTask = mock(InternalPartitionServiceImpl.MigrateTask.class);

        migrationQueue.add(migrateTask);
        migrationQueue.afterTaskCompletion(migrateTask);

        assertFalse(migrationQueue.hasMigrationTasks());
    }

    @Test
    public void test_migrateTaskCount_notDecremented_afterNonMigrateTaskCompleted()
            throws InterruptedException {
        migrationQueue.add(mock(InternalPartitionServiceImpl.MigrateTask.class));
        migrationQueue.afterTaskCompletion(mock(Runnable.class));

        assertTrue(migrationQueue.hasMigrationTasks());
    }

    @Test
    public void test_migrateTaskCount_decremented_onClear() {
        migrationQueue.add(mock(InternalPartitionServiceImpl.MigrateTask.class));
        migrationQueue.clear();

        assertFalse(migrationQueue.hasMigrationTasks());
    }

    @Test(expected = IllegalStateException.class)
    public void test_migrateTaskCount_notDecremented_belowZero() {
        migrationQueue.afterTaskCompletion(mock(InternalPartitionServiceImpl.MigrateTask.class));
    }

}
