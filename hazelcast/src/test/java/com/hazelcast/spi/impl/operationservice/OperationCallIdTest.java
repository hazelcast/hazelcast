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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import junit.framework.AssertionFailedError;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationCallIdTest {

    @Rule
    public final ExpectedException exceptionRule = ExpectedException.none();

    private final Operation op = new MockOperation();

    @Test
    public void when_callIdNotSet_thenIsZero() {
        assertEquals(0, op.getCallId());
    }

    @Test
    public void when_setCallIdInitial_thenActive() {
        op.setCallId(1);
        assertEquals(1, op.getCallId());
        assertTrue(op.isActive());
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_setZeroCallId_thenFail() {
        op.setCallId(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_setNegativeCallId_thenFail() {
        op.setCallId(-1);
    }

    @Test
    public void when_setCallIdOnActiveOp_thenFail() {
        // Given
        op.setCallId(1);
        assertTrue(op.isActive());

        // Then
        exceptionRule.expect(IllegalStateException.class);

        // When
        op.setCallId(1);
    }

    @Test
    public void when_deactivateActiveOp_thenInactive() {
        // Given
        op.setCallId(1);
        assertTrue(op.isActive());

        // When
        op.deactivate();

        // Then
        assertFalse(op.isActive());
    }

    @Test
    public void when_deactivateInitialStateOp_thenNothingHappens() {
        // Given
        assertFalse(op.isActive());

        // When
        op.deactivate();

        // Then
        assertFalse(op.isActive());
    }

    @Test
    public void when_deactivateDeactivatedOp_thenNothingHappens() {
        // Given
        op.setCallId(1);
        op.deactivate();
        assertFalse(op.isActive());

        // When
        op.deactivate();

        // Then
        assertFalse(op.isActive());
    }

    @Test
    public void when_getCallIdOnDeactivatedOp_thenCallIdPreserved() {
        // Given
        final int mockCallId = 1;
        op.setCallId(mockCallId);
        op.deactivate();

        // When
        long callId = op.getCallId();

        // Then
        assertEquals(callId, mockCallId);
    }

    @Test
    public void when_setCallIdOnDeactivatedOp_thenReactivated() {
        // Given
        op.setCallId(1);
        op.deactivate();

        // When
        int newCallId = 2;
        op.setCallId(newCallId);

        // Then
        assertTrue(op.isActive());
        assertEquals(newCallId, op.getCallId());
    }

    @Test
    public void when_concurrentlySetCallId_thenOnlyOneSucceeds() {
        new ConcurrentExcerciser().run();
    }

    class MockOperation extends Operation {

        @Override
        public void run() throws Exception {
        }
    }

    class ConcurrentExcerciser {
        final Operation op = new MockOperation();
        volatile AssertionFailedError testFailure;
        volatile int activationCount;

        void run() {
            final Thread reader = new Thread() {
                @Override
                public void run() {
                    try {
                        long previousCallId = 0;
                        while (!interrupted()) {
                            long callId;
                            while ((callId = op.getCallId()) == previousCallId) {
                                // Wait until a writer thread sets a call ID
                            }
                            activationCount++;
                            long deadline = System.currentTimeMillis() + SECONDS.toMillis(1);
                            while (System.currentTimeMillis() < deadline) {
                                assertEquals(callId, op.getCallId());
                            }
                            op.deactivate();
                            previousCallId = callId;
                        }
                    } catch (AssertionFailedError e) {
                        testFailure = e;
                    }
                }
            };
            final int writerCount = 4;
            final Thread[] writers = new Thread[writerCount];
            for (int i = 0; i < writers.length; i++) {
                final int initialCallId = i + 1;
                writers[i] = new Thread() {
                    @Override
                    public void run() {
                        long nextCallId = initialCallId;
                        while (!interrupted()) {
                            try {
                                op.setCallId(nextCallId);
                                nextCallId += writerCount;
                            } catch (IllegalStateException e) {
                                // Things are working as expected
                            }
                        }
                    }
                };
            }
            try {
                reader.start();
                for (Thread t : writers) {
                    t.start();
                }
                final long deadline = System.currentTimeMillis() + SECONDS.toMillis(5);
                while (System.currentTimeMillis() < deadline) {
                    if (testFailure != null) {
                        throw testFailure;
                    }
                    LockSupport.parkNanos(MILLISECONDS.toNanos(1));
                }
            } finally {
                reader.interrupt();
                for (Thread t : writers) {
                    t.interrupt();
                }
            }
            assertTrue("Failed to activate the operation at least twice", activationCount >= 2);
        }
    }
}
