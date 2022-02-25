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

package com.hazelcast.jet.core.test;

import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TestOutboxTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void when_differentObjectAfterFalse_then_rejected() {
        TestOutbox outbox = new TestOutbox(1);
        assertTrue(outbox.offer(1));
        assertFalse(outbox.offer(2));
        outbox.drainQueueAndReset(0, new ArrayList<>(), false);

        // Then
        exception.expectMessage("Different");
        // When
        assertFalse(outbox.offer(3));
    }

    @Test
    public void when_differentObjectToSnapshotAfterFalse_then_rejected() {
        TestOutbox outbox = new TestOutbox(new int[]{1}, 1);
        assertTrue(outbox.offerToSnapshot("k1", "v1"));
        assertFalse(outbox.offerToSnapshot("k2", "v2"));
        outbox.drainSnapshotQueueAndReset(new ArrayList<>(), false);

        // Then
        exception.expectMessage("Different");
        // When
        assertFalse(outbox.offerToSnapshot("k3", "v3"));
    }

    @Test
    public void when_resetCalled_then_offerSucceeds() {
        TestOutbox outbox = new TestOutbox(1);
        assertTrue(outbox.offer(1));
        assertFalse(outbox.offer(2));

        // When
        outbox.reset();
        // Then - this would fail if reset() was required
        assertFalse(outbox.offer(2));
    }

    @Test
    public void when_resetNotCalled_then_offerFails() {
        TestOutbox outbox = new TestOutbox(1);
        // When
        assertTrue(outbox.offer(1));
        assertFalse(outbox.offer(2));

        // Then
        exception.expectMessage("offer() called again");
        assertFalse(outbox.offer(2));
    }

    @Test
    public void when_offeredToMinusOne_then_offeredToAll() {
        TestOutbox outbox = new TestOutbox(2);
        assertTrue(outbox.offer(-1, "foo"));
    }

    @Test
    public void when_notSerializable_then_fails() {
        // this is meant to test that the object offered to snapshot is actually serialized
        TestOutbox outbox = new TestOutbox(new int[0], 1);
        exception.expect(HazelcastSerializationException.class);
        outbox.offerToSnapshot("k", new Object());
    }
}
