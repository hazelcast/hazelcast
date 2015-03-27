package com.hazelcast.partition.impl;

import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.hazelcast.core.MigrationEvent.MigrationStatus.COMPLETED;
import static com.hazelcast.core.MigrationEvent.MigrationStatus.FAILED;
import static com.hazelcast.core.MigrationEvent.MigrationStatus.STARTED;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MigrationListenerAdapterTest {

    @Mock
    private MigrationListener listener;

    private MigrationListenerAdapter adapter;

    @Before
    public void init() {
        adapter = new MigrationListenerAdapter(listener);
    }

    @Test
    public void test_migrationStarted() {
        final MigrationEvent event = new MigrationEvent(0, null, null, STARTED);
        adapter.onEvent(event);

        verify(listener).migrationStarted(event);
        verify(listener, never()).migrationCompleted(any(MigrationEvent.class));
        verify(listener, never()).migrationFailed(any(MigrationEvent.class));
    }

    @Test
    public void test_migrationCompleted() {
        final MigrationEvent event = new MigrationEvent(0, null, null, COMPLETED);
        adapter.onEvent(event);

        verify(listener, never()).migrationStarted(any(MigrationEvent.class));
        verify(listener).migrationCompleted(event);
        verify(listener, never()).migrationFailed(any(MigrationEvent.class));
    }

    @Test
    public void test_migrationFailed() {
        final MigrationEvent event = new MigrationEvent(0, null, null, FAILED);
        adapter.onEvent(event);

        verify(listener, never()).migrationStarted(any(MigrationEvent.class));
        verify(listener, never()).migrationCompleted(any(MigrationEvent.class));
        verify(listener).migrationFailed(event);
    }

}
