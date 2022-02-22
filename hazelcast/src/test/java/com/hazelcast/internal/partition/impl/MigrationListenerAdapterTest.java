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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.internal.partition.ReplicaMigrationEventImpl;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static com.hazelcast.internal.partition.impl.MigrationListenerAdapter.MIGRATION_FINISHED_PARTITION_ID;
import static com.hazelcast.internal.partition.impl.MigrationListenerAdapter.MIGRATION_STARTED_PARTITION_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MigrationListenerAdapterTest {

    @Mock
    private MigrationListener listener;

    private MigrationListenerAdapter adapter;

    @Before
    public void init() {
        adapter = new MigrationListenerAdapter(listener);
    }

    @Test
    public void test_migrationProcessStarted() {
        MigrationState migrationSchedule = new MigrationStateImpl();
        ReplicaMigrationEvent event = new ReplicaMigrationEventImpl(migrationSchedule, MIGRATION_STARTED_PARTITION_ID, 0, null, null, true, 0L);
        adapter.onEvent(event);

        verify(listener).migrationStarted(migrationSchedule);
        verify(listener, never()).migrationFinished(any(MigrationStateImpl.class));
        verify(listener, never()).replicaMigrationCompleted(any(ReplicaMigrationEvent.class));
        verify(listener, never()).replicaMigrationFailed(any(ReplicaMigrationEvent.class));
    }

    @Test
    public void test_migrationProcessCompleted() {
        MigrationState migrationSchedule = new MigrationStateImpl();
        ReplicaMigrationEvent event = new ReplicaMigrationEventImpl(migrationSchedule, MIGRATION_FINISHED_PARTITION_ID, 0, null, null, true, 0L);
        adapter.onEvent(event);

        verify(listener, never()).migrationStarted(any(MigrationState.class));
        verify(listener).migrationFinished(migrationSchedule);
        verify(listener, never()).replicaMigrationCompleted(any(ReplicaMigrationEvent.class));
        verify(listener, never()).replicaMigrationFailed(any(ReplicaMigrationEvent.class));
    }

    @Test
    public void test_migrationCompleted() {
        MigrationState migrationSchedule = new MigrationStateImpl();
        ReplicaMigrationEvent event = new ReplicaMigrationEventImpl(migrationSchedule, 0, 0, null, null, true, 0L);
        adapter.onEvent(event);

        verify(listener, never()).migrationStarted(any(MigrationState.class));
        verify(listener, never()).migrationFinished(any(MigrationState.class));
        verify(listener, never()).replicaMigrationFailed(any(ReplicaMigrationEvent.class));
        verify(listener).replicaMigrationCompleted(event);
    }

    @Test
    public void test_migrationFailed() {
        MigrationState migrationSchedule = new MigrationStateImpl();
        ReplicaMigrationEvent event = new ReplicaMigrationEventImpl(migrationSchedule, 0, 0, null, null, false, 0L);
        adapter.onEvent(event);

        verify(listener, never()).migrationStarted(any(MigrationState.class));
        verify(listener, never()).migrationFinished(any(MigrationState.class));
        verify(listener, never()).replicaMigrationCompleted(any(ReplicaMigrationEvent.class));
        verify(listener).replicaMigrationFailed(event);
    }
}
