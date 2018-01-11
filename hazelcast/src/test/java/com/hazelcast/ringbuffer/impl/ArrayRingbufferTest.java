/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ArrayRingbufferTest {
    @Test(expected = StaleSequenceException.class)
    public void testReadStaleSequenceThrowsException() {
        final ArrayRingbuffer rb = fullRingbuffer();
        rb.read(rb.headSequence() - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testReadFutureSequenceThrowsException() {
        final ArrayRingbuffer rb = fullRingbuffer();
        rb.read(rb.tailSequence() + 1);
    }

    @Test(expected = StaleSequenceException.class)
    public void testBlockableReadStaleSequenceThrowsException() {
        final ArrayRingbuffer rb = fullRingbuffer();
        rb.checkBlockableReadSequence(rb.headSequence() - 1);
    }

    public void testBlockableReadFutureSequenceOk() {
        final ArrayRingbuffer rb = fullRingbuffer();
        rb.checkBlockableReadSequence(rb.tailSequence() + 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBlockableReadFutureSequenceThrowsException() {
        final ArrayRingbuffer rb = fullRingbuffer();
        rb.checkBlockableReadSequence(rb.tailSequence() + 2);
    }

    @Test
    public void testIsEmpty() {
        final ArrayRingbuffer rb = new ArrayRingbuffer(5);
        assertTrue(rb.isEmpty());
        rb.add("");
        assertFalse(rb.isEmpty());
    }

    private static ArrayRingbuffer fullRingbuffer() {
        final ArrayRingbuffer rb = new ArrayRingbuffer(5);
        for (int i = 0; i < rb.getCapacity(); i++) {
            rb.add("");
        }
        return rb;
    }
}
