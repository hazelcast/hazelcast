/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static com.hazelcast.core.MigrationEvent.MigrationStatus.COMPLETED;
import static com.hazelcast.core.MigrationEvent.MigrationStatus.FAILED;
import static com.hazelcast.core.MigrationEvent.MigrationStatus.STARTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@Category(QuickTest.class)
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

    @Test
    public void test_migrationEvent_serialization()
            throws IOException {
        final MigrationEvent event = new MigrationEvent(0, null, null, STARTED);

        final ObjectDataOutput output = mock(ObjectDataOutput.class);
        event.writeData(output);

        verify(output).writeInt(0);
        verify(output, times(2)).writeObject(null);
        verify(output).writeByte(0);
    }

    @Test
    public void test_migrationEvent_deserialization()
            throws IOException {
        final ObjectDataInput input = mock(ObjectDataInput.class);
        when(input.readInt()).thenReturn(0);
        when(input.readObject()).thenReturn(null);
        when(input.readObject()).thenReturn(null);
        when(input.readByte()).thenReturn((byte) 0);

        final MigrationEvent event = new MigrationEvent();
        event.readData(input);

        assertEquals(0, event.getPartitionId());
        assertNull(event.getOldOwner());
        assertNull(event.getNewOwner());
        assertEquals(STARTED, event.getStatus());
    }

}
