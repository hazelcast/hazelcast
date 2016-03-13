/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 *
 */

package com.hazelcast.internal.partition.impl;

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
